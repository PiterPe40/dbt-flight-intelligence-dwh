FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev git && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files
COPY --chown=airflow:airflow dags/ /opt/airflow/dags/
COPY --chown=airflow:airflow dbt_project/ /opt/airflow/dbt_project/
COPY --chown=airflow:airflow scripts/ /opt/airflow/scripts/

WORKDIR /opt/airflow
