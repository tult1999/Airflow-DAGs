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
def load_data_to_postgres(conn_id: str, file_folder:str, file_name: str, table: str):
    """
    * Main infomation:
    Name: postgres_destination_load_data
    Used for: Loading data from a flat .csv file to PostgreSQL Database
    Conditions to use: 
        (1) Connection already set-up on Apache Airflow, you can check on the Airflow server with the command 'airflow connections list'
        (2) PostgreSQL Database server is on
        (3) The connection works fine

    * Parameters:
    param conn_id: The connection ID of the connection established between Apache Airflow server and MongoDB database
    param file_folder: The name of the folder of the imported file
    param file_name: The name of the imported file
    param table: The table name to be imported to
    """
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    file_folder = file_folder
    file_name = file_name
    table = table
    path = f'/Datafile/Outputfile/{file_folder}/{file_name}'

    query = f"COPY {table} FROM '{path}' DELIMITER ',';"

    pg_hook.run(sql=query, autocommit=True)

# BLOCK2: Config Parameters and Execute Pythoncallables
def exec_olist_postgres_destination_load_data():
    __name__ = '__main__' # Run this when meet the Error: Pythoncallable is not callable
    if __name__ == '__main__':
        load_data_to_postgres(conn_id='my_postgres_db', file_folder='Olist', file_name='Customers.csv', table='Customers')

with DAG('postgres_destination_load_data', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    postgres_destination_load_data = PythonOperator(
        task_id='postgres_destination_load_data',
        python_callable=exec_olist_postgres_destination_load_data
    )

    postgres_destination_load_data
