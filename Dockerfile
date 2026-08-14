FROM jupyter/pyspark-notebook:spark-3.4.0

USER root

RUN pip install --no-cache-dir \
    "apache-airflow==2.9.1" \
    "papermill==2.5.0" \
    "delta-spark==2.4.0" \
    "psycopg2-binary==2.9.9" \
    "pandas==2.0.3" \
    "pyarrow==12.0.1"

RUN mkdir -p /usr/local/spark/jars && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
         -O /usr/local/spark/jars/postgresql-42.7.3.jar

# Pre-create Airflow home so the volume mount for dags does not cause root ownership
RUN mkdir -p /home/jovyan/airflow/dags \
              /home/jovyan/airflow/logs \
              /home/jovyan/airflow/plugins && \
    chown -R jovyan:users /home/jovyan/airflow

USER jovyan
