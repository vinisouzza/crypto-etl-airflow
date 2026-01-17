import json
import pandas as pd
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def _transform_and_load(ti):
    """
    Puxa o JSON extraído, transforma com Pandas e carrega no Postgres.
    'ti' (Task Instance) é injetado pelo Airflow para permitir XComs.
    """
    print("Iniciando transformação e carga de dados...")

    # 1. Puxar (pull) o resultado da tarefa anterior (task_extract) Via XCom
    json_data = ti.xcom_pull(task_ids='task_extract_crypto_data')

    if not json_data:
        raise ValueError("Nenhum dado JSON recebido para transformação.")
    
    # 2. Transformar os dados com Pandas
    # A API retorna {'bitcoin': {'usd': 60000}, 'ethereum': {'usd': 4000}, ...}

    try:
        data = json_data
        df = pd.DataFrame.from_dict(data, orient='index')
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'crypto_id', 'usd': 'price_usd'}, inplace=True) 

        # Adiciona timestamp
        df['extracted_at'] = datetime.now()

        print("Dados transformados:")
        print(df.head())
    except Exception as e:
        raise ValueError(f"Erro na transformação dos dados: {e}")
    
    # 3. Carregar no Postgres
    # Hook para interagir com a conexão 'postgres_dw'
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Upsert (Inserir ou Atualizar se já existir naquele horário)
    # Isso garante que se rodar 2x não duplica dados (Idempotência)

    for _, row in df.iterrows():
        sql = """
            INSERT INTO crypto_prices (crypto_id, price_usd, extracted_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (crypto_id, extracted_at) DO NOTHING;
        """
        cursor.execute(sql, (row['crypto_id'], row['price_usd'], row['extracted_at']))
    
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    'crypto_etl_pipeline_psql',
    default_args=default_args,
    schedule='@hourly',
    description='Pipeline ETL para dados de criptomoedas da CoinGecko usando Airflow, Pandas e Postgres',
    catchup=False,
    tags=['etl', 'api', 'projeto', ],
) as dag:
    
    # Tarefa 1: Sensor - Verifica se a API está disponível
    task_check_api = HttpSensor(
        task_id='task_check_api_online',
        http_conn_id='http_coingecko', # ID da conexão HTTP configurada no Airflow
        endpoint='api/v3/ping' ,      # Endpoint para checar
        response_check=lambda response: response.status_code == 200 and "gecko" in response.text,
        poke_interval=5, # checa a cada 5 segundos
        timeout=20,      # tempo máximo de espera 20 segundos
    )

    # Tarefa 2: Operator - Criar a tabela no Postgres se não existir
    task_create_table = PostgresOperator(
        task_id='task_create_table',
        postgres_conn_id='postgres_dw', # ID da conexão Postgres configurada no Airflow
        sql="""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            crypto_id TEXT,
            price_usd REAL,
            extracted_at TIMESTAMP,
            PRIMARY KEY (crypto_id, extracted_at)
        );
        """
    )

    # Tarefa 3: Operator - Extrair dados da API CoinGecko
    # HttpOperator para fazer a requisição HTTP GET
    # Por padrão, ele "dá push" da resposta JSON para o XCom
    task_extract_crypto_data = HttpOperator(
        task_id='task_extract_crypto_data',
        http_conn_id='http_coingecko',
        endpoint='api/v3/simple/price',
        method='GET',
        data={
            'ids': 'bitcoin,ethereum,dogecoin',
            'vs_currencies': 'usd'
        },
        response_filter=lambda response: json.loads(response.text), # Processa a resposta JSON
        log_response=True,
    )

    # Tarefa 4: Operator - Transformar e carregar os dados
    task_transform_and_load = PythonOperator(
        task_id='task_transform_and_load',
        python_callable=_transform_and_load,
    )

    # Definindo o fluxo do pipeline
    task_check_api >> task_create_table >> task_extract_crypto_data >> task_transform_and_load