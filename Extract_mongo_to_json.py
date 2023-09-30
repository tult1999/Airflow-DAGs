import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data_and_save_to_file():
    mongo_hook = MongoHook(conn_id='my_mongodb')
    collection = 'Customers'
    query = {"customer_city": "pacaja"}  # Your query to filter the data

    records = mongo_hook.find(collection, query=query, projection={ "_id": 0})

    # Convert records to a list of dictionaries
    data = [record for record in records]

    # Save data to a JSON file
    output_file = '/opt/airflow/dags/Datafile/Olist/Customers.json'
    with open(output_file, 'w') as file:
        json.dump(data, file)

with DAG('mongo_extraction_dag', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    task_extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_and_save_to_file
    )

    task_extract_data