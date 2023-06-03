import json
import csv
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# BLOCK1: Built-in Pythoncallables
def extract_data_from_postgres():
    conn_id='my_postgres_db'
    connector_name = 'Postgres_Olist_Customers'
    table='Customers'
    """
    * Main infomation:
    Name: extract_data_from_postgres
    Used for: Extracting data from PostgreSQL Database to a flat .csv file
    Conditions to use: 
        (1) Connection already set-up on Apache Airflow, you can check on the Airflow server with the command 'airflow connections list'
        (2) PostgreSQL Database server is on
        (3) The connection works fine

    * Parameters:
    param conn_id: The connection ID of the connection established between Apache Airflow server and MongoDB database
    param connector_name: the name of built connector
    param table: The table name to be imported to
    """
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    table = table
    output_file_path = f'/opt/airflow/dags/Datafile/Inputfile/Olist/Connector_{connector_name}.csv'
    query = f"SELECT * FROM {table};"
    
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)

    data = cursor.fetchall()

    with open(output_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)
    
    cursor.close()
    connection.close()
    
# BLOCK2: Build DAGs
with DAG('postgres_extract_data', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    extract_data_from_postgres = PythonOperator(
        task_id='extract_data_from_postgres',
        python_callable=extract_data_from_postgres
    )

    extract_data_from_postgres
