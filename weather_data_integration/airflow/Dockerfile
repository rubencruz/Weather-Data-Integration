FROM apache/airflow:2.10.2-python3.11

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir \
    --constraint /home/airflow/constraints.txt \
    -r /requirements.txt