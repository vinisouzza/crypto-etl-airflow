# Pipeline ETL de Criptomoedas com Airflow, Postgres e Metabase

Este projeto implementa um pipeline completo de Engenharia de Dados que extrai preços de criptomoedas a cada hora, armazena em um Data Warehouse no PostgreSQL, valida a qualidade dos dados usando Soda Core e disponibiliza visualizações no Metabase.

O objetivo é monitorar a variação horária do **Bitcoin**, **Ethereum**, **Tether** e **Solana**, permitindo acompanhar tendências.

---

## Arquitetura e Fluxo
1. **Disponibilidade (Sensor)**: Verifica se a API da CoinGecko está online antes de iniciar o processamento (evita falhas "sujas")

2. **Extração (Extract)**: Coleta dados de criptomoedas (Bitcoin, Ethereum, Tether e Solana) via `HttpOperator`.

3. **Transformação (Transform)**: Os dados são processados com Pandas: normalização, flatten do JSON e inclusão de timestamp (UTC).

4. **Carga (Load)**: Inserção em lote (Batch Insert) no PostgreSQL utilizando `executemany`.

5. **Qualidade de Dados (Quality Gate)**: O Soda Core realiza uma auditoria automática nos dados recém-inseridos. Se regras de negócio forem violadas (ex: preço negativo, dados não recentes), o pipeline falha e alerta.

6. **Visualização**: Dashboards no Metabase conectados ao Data Warehouse.

---

## Tecnologias Utilizadas

| Componente | Tecnologia |
|------------|------------|
| Orquestração | Apache Airflow 2.10 (Docker) |
| Qualidade de Dados | Soda Core (YAML based checks) |
| Linguagem | Python 3.12 (Pandas) |
| Banco de Dados | PostgreSQL 13 |
| Visualização | Metabase |
| Infraestrutura | Docker & Docker Compose |

---

## Diagrama e Dashboard

### **Diagrama do Pipeline**
![Pipeline](img/etl-diagram.png)

### **Dashboard no Metabase**
![Dashboard](img/metabase-dashboard.png)

> *Obs: Dashboard ilustrativo apenas com 9 horas de captura de dados.*

---

## Como Executar

### **Pré-requisitos**
- Docker e Docker Compose instalados.
- Chave de API CoinGecko (Demo gratuita).

### **Passo a Passo**

### 1. Clone o repositório
```bash
git clone https://github.com/vinisouzza/crypto-etl-airflow.git
cd crypto-etl-airflow
```

### 2. Suba o ambiente
Este comando irá construir a imagem customizada do Airflow (com as libs necessárias) e iniciar os serviços (Postgres, Metabase, etc).
```bash
docker-compose up -d --build
```
*Aguarde alguns instantes até que todos os containers estejam com status "Healthy".*

### 3. Configure a Conexão no Airflow
1. Acesse `http://localhost:8080` (Login: `airflow` / Senha: `airflow`).
2. Vá em **Admin** -> **Connections** e clique no **+**.

**A. Conexão da API (CoinGecko):**
   - **Conn Id:** `http_coingecko`
   - **Conn Type:** `HTTP`
   - **Host:** `https://api.coingecko.com`
   - **Extra:** `{"x-cg-demo-api-key": "SUA_API_KEY_AQUI"}`

**B. Conexão do Banco (Postgres):**
   - **Conn Id:** `postgres_dw`
   - **Conn Type:** `Postgres`
   - **Host:** `postgres`
   - **Login:** `airflow`
   - **Password:** `airflow`
   - **Schema:** `airflow`
   - **Port:** `5432`

### 4. Arquivos de Qualidade (Soda)
As regras de qualidade estão definidas em `include/soda/checks.yml`. Você pode ajustá-las localmente e o Airflow reconhecerá automaticamente via Docker Volume.

### 5. Execute a DAG
Na tela inicial, ative a DAG `crypto_etl_pipeline_psql` (botão ON/OFF).
Acompanhe o log da task data_quality_gate para ver o relatório de validação.

---

### Metabase (Analytics)
- Acesse `http://localhost:3000`.
- Crie sua conta de administrador.
- Na configuração de banco de dados, selecione PostgreSQL e use:
    - **Host**: `postgres`
    - **Database name**: `airflow`
    - **Username**: `airflow`
    - **Password**: `airflow`
- Crie seus gráficos explorando a tabela crypto_prices.
