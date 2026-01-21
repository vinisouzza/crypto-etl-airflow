import json
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from soda.scan import Scan

# Configuração de logging
logger = logging.getLogger(__name__)

# Configurações constantes
COINS = ['bitcoin', 'ethereum', 'tether', 'solana']
CURRENCY = 'usd'
POSTGRES_CONN_ID = 'postgres_dw'  # Deve ser configurada no Airflow Connections
API_CONN_ID = 'http_coingecko'  # Deve ser configurada no Airflow Connections

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

def _transform_data(ti) -> Dict[str, Any]:
    """
    Recebe o JSON bruto da API, normaliza com Pandas e retorna um dicionário
    serializável para o XCom.
    """
    logger.info("Iniciando transformação de dados.")

    # XCom Pull: Pega o output da task de extração
    json_data = ti.xcom_pull(task_ids='task_extract_crypto_data')

    if not json_data:
        raise ValueError("Nenhum dado JSON recebido da API.")
    
    
    try:
        # Transforma dicionário aninhado em tabular
        df = pd.DataFrame.from_dict(json_data, orient='index')
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'crypto_id', 'usd': 'price_usd'}, inplace=True) 

        # Adiciona timestamp
        df['extracted_at'] = datetime.now().isoformat()

        logger.info(f"Transformação concluída. Linhas processadas: {len(df)}")
        
        # Retorna como dicionário para o XCom
        return df.to_dict(orient='records')
    
    except Exception as e:
        logger.error(f"Erro na transformação dos dados: {e}")
        raise
    
def _load_data_to_postgres(ti):
    """
    Pega os dados transformados do XCom e faz um Upsert no Postgres.
    """
    logger.info("Iniciando carregamento de dados no Postgres.")
    
    # XCom Pull: Pega o output da task de transformação
    data_records = ti.xcom_pull(task_ids='task_transform_data')

    if not data_records:
        raise ValueError("Nenhum dado transformado disponível para carregar.")
    
    # Conecta no banco
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Query de UPSERT (Idempotência)
    sql = """
            INSERT INTO crypto_prices (crypto_id, price_usd, extracted_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (crypto_id, extracted_at) DO NOTHING;
    """

    try:
        # Prepara a lista de tuplas para inserção
        data_tuples = [
            (r['crypto_id'], r['price_usd'], r['extracted_at']) for r in data_records
        ]

        cursor.executemany(sql, data_tuples)
        conn.commit()
        logger.info(f"Carga finalizada. Registros inseridos: {len(data_tuples)}")
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao carregar dados no Postgres: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
    
def run_soda_scan():
    """
    Executa testes de qualidade de dados usando Soda Core.
    """
    logger.info("Iniciando verificação de qualidade de dados com Soda Scan.")

    scan = Scan()
    scan.set_data_source_name("postgres_coingecko")
    
    # Caminhos para os arquivos YAML
    scan.add_configuration_yaml_file("/opt/airflow/include/soda/configuration.yml")
    scan.add_sodacl_yaml_file("/opt/airflow/include/soda/checks.yml")
    
    # Executa a verificação
    exit_code = scan.execute()

    # Imprime logs detalhados no Airflow
    logger.info(scan.get_logs_text())
    
    # Se houver falhas, gera logs e para a DAG
    if exit_code != 0:
        raise ValueError("Soda Quality Gate Failed! Dados inconsistentes detectados.")
    
# Definição da DAG
with DAG(
    'crypto_etl_pipeline_psql',
    default_args=default_args,
    schedule='@hourly',
    description='Pipeline ETL CoinGecko: Extract -> Transform -> Load -> Quality Check',
    catchup=False,
    tags=['etl', 'crypto', 'postgres'],
) as dag:
    
    # 1. Sensor: Check API Availability
    task_check_api = HttpSensor(
        task_id='task_check_api_online',
        http_conn_id=API_CONN_ID,
        endpoint='api/v3/ping' ,      # Endpoint para checar
        response_check=lambda response: response.status_code == 200 and "gecko" in response.text,
        poke_interval=10, # checa a cada 10 segundos
        timeout=60,      # tempo máximo de espera 60 segundos
        mode='reschedule' # 'reschedule' libera o worker slot enquanto espera (melhor performance)
    )

    # 2. DDL: Create Table Schema
    task_create_table = PostgresOperator(
        task_id='task_create_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            crypto_id TEXT,
            price_usd REAL,
            extracted_at TIMESTAMP,
            PRIMARY KEY (crypto_id, extracted_at)
        );
        """
    )

    # 3. Extract: Get Data from API
    task_extract = HttpOperator(
        task_id='task_extract_crypto_data',
        http_conn_id=API_CONN_ID,
        endpoint='api/v3/simple/price',
        method='GET',
        data={
            'ids': ','.join(COINS),
            'vs_currencies': CURRENCY
        },
        response_filter=lambda response: json.loads(response.text), # Processa a resposta JSON
        log_response=True,
    )

    # 4. Transform: Clean and Normalize (Python Puro)
    task_transform = PythonOperator(
        task_id='task_transform_data',
        python_callable=_transform_data,
    )

    # 5. Load: Insert into DB
    task_load = PythonOperator(
        task_id='task_load_to_postgres',
        python_callable=_load_data_to_postgres,
    )

    # 6. Quality Gate: Validate Data
    task_quality = PythonOperator(
        task_id='data_quality_gate',
        python_callable=run_soda_scan
    )

    # Orquestração
    task_check_api >> task_create_table >> task_extract >> task_transform >> task_load >> task_quality