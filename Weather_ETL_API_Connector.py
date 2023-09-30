import json
import pandas as pd
import requests as req
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
def api_connector_extract_data():
    url='https://weatherapi-com.p.rapidapi.com/current.json'
    headers={
            'X-RapidAPI-Key': '333d11a99fmsh0f40176e4390370p10fbdfjsn1d464c29f14b',
            'X-RapidAPI-Host': 'weatherapi-com.p.rapidapi.com'
            }
    params={'q': '53.1,-0.13'}
    file_name= 'weather_data.json'
    """
    * Main infomation:
    Name: api_connector_extract_data
    Used for: Extracting data from an API and save the data into a json file
    Conditions to use: 
        (1) Connection already set-up on Apache Airflow, you can check on the Airflow server with the command 'airflow connections list'
        (2) The API is tested and fine to use

    * Parameters:
    param url: The URL of the API to be called
    param headers: The headers of the API
    param params: The params of the API
    param file_name: The file name to be exported data to
    """
    api_result = req.get(url, headers=headers, params=params)
    json_data = api_result.json()
    with open(f"/opt/airflow/dags/Datafile/Inputfile/API_testing/{file_name}", 'w') as outputfile:
        json.dump(json_data, outputfile)

# BLOCK2: Build DAGs
with DAG('api_connector_extract_data', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    api_connector_extract_data = PythonOperator(
        task_id='api_connector_extract_data',
        python_callable=api_connector_extract_data
    )

    api_connector_extract_data