import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
from json import loads, dumps

default_args = {
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# BLOCK1: Built-in Pythoncallables
def mongo_destination_load_data():
    conn_id='my_mongodb'
    collection='Sellers'
    input_file_path='/opt/airflow/dags/Datafile/Inputfile/Olist/olist_sellers_dataset.csv'
    """
    * Main infomation:
    Name: mongo_destination_load_data
    Used for: Load data into a MongoDB Database collection
    Conditions to use: 
        (1) Connection already set-up on Apache Airflow, you can check on the Airflow server with the command 'airflow connections list'
        (2) MongoDB Database server is on
        (3) The connection works fine

    * Parameters:
    param conn_id: The connection ID of the connection established between Apache Airflow server and MongoDB database
    param collection: The name of the Collection from where you want to take data
    param input_file_path: The path of the file to import
    """
    mongo_hook = MongoHook(conn_id)
    collection = collection

    data = pd.read_csv(input_file_path)
    result = data.to_json(orient='records')
    json_data = loads(result)

    mongo_hook.insert_many(mongo_collection=collection, docs=json_data)

# BLOCK2: Build DAGs
with DAG('mongo_destination_load_data', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    mongo_destination_load_data = PythonOperator(
        task_id='mongo_destination_load_data',
        python_callable=mongo_destination_load_data
    )

    mongo_destination_load_data