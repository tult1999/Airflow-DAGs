import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# BLOCK1: Built-in Pythoncallables
def load_data_to_postgres(conn_id: str, file_folder: str, file_name: str, table: str):
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    file_folder = file_folder
    file_name = file_name
    table = table
    path = f'/Datafile/Outputfile/{file_folder}/{file_name}'

    query = f"COPY {table} FROM '{path}' DELIMITER ',';"

    pg_hook.run(sql=query, autocommit=True)

# BLOCK2: Config Parameters Pythoncallables
def exec_olist_postgres_destination_load_data():
    if 1 != 0:
        load_data_to_postgres()

with DAG('postgres_destination_load_data', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    postgres_destination_load_data = PythonOperator(
        task_id='postgres_destination_load_data',
        python_callable=exec_olist_postgres_destination_load_data
    )

    postgres_destination_load_data
