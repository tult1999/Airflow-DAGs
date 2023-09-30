import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_data_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_db')
    file_folder = 'Olist'
    file_name = 'Customers.csv'
    table = 'Customers'
    path = f'/Datafile/Outputfile/{file_folder}/{file_name}'

    query = f"COPY {table} FROM '{path}' DELIMITER ',';"

    pg_hook.run(sql=query, autocommit=True)

def test():
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_db')
    file_folder = 'Olist'
    file_name = 'Customers.json'
    table = 'Customers'
    path = f'/Datafile/Outputfile/{file_folder}/{file_name}'

    query = f"COPY {table} FROM '{path}' DELIMITER ',';"

    print(query)

# def dump_into_csv():
#     data = pd.read_json('/opt/airflow/dags/Datafile/Olist/Customers.json', orient='records')

#     data.to_csv('/opt/airflow/dags/Datafile/Olist/Customers.csv', index=False)

with DAG('postgres_import_from_file', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    task_import_data = PythonOperator(
        task_id='import_from_file',
        python_callable=load_data_to_postgres
    )

    task_test = PythonOperator(
        task_id='test',
        python_callable=test
    )

    # task_dump_into_csv = PythonOperator(
    #     task_id='dump_into_csv',
    #     python_callable=dump_into_csv
    # )

    task_import_data