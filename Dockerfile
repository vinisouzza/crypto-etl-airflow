FROM apache/airflow:2.10.2

COPY requirements.txt /requirements.txt

# Instala as dependências extras
RUN pip install --no-cache-dir -r /requirements.txt