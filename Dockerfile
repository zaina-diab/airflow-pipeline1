FROM apache/airflow:2.10.2-python3.11

USER root

# تثبيت dependencies للنظام (مهم أحيانًا)
RUN apt-get update && apt-get install -y gcc

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

COPY config.yaml /opt/airflow/config.yaml
COPY airflow/etl/ /opt/airflow/etl/