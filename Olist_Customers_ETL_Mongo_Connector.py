import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# BLOCK1: Built-in Pythoncallables
def mongo_connector_extract_data(conn_id:str, collection:str, query:dict, projection:dict,
    output_file_path:str):
    """
    * Main infomation:
    Name: mongo_connector_extract_data
    Used for: Extracting data from a MongoDB Database
    Conditions to use: 
        (1) Connection already set-up on Apache Airflow, you can check on the Airflow server with the command 'airflow connections list'
        (2) MongoDB Database server is on
        (3) The connection works fine

    * Parameters:
    param conn_id: The connection ID of the connection established between Apache Airflow server and MongoDB database
    param collection: The name of the Collection from where you want to take data
    param query: The filter condition to select data to be exported
    param projection: The condition to select columns to be exported
    param output_file_path: The path of the file to be exported to
    """
    mongo_hook = MongoHook(conn_id)
    collection = collection
    query = query  # Your query to filter the data

    records = mongo_hook.find(collection, query=query, projection=projection)

    # Convert records to a list of dictionaries
    data = [record for record in records]

    # Convert the list of dictionaries to a Pandas Dataframe
    output_file = output_file_path
    data_csv = pd.DataFrame(data)
    data_csv.head()
    data_csv.to_csv(path_or_buf=output_file, index=False)

# BLOCK2: Config Parameters and Execute Pythoncallables
def exec_olist_mongo_connector_extract_data():
    __name__ = '__main__'
    if __name__ == '__main__':
        mongo_connector_extract_data(conn_id='my_mongodb', collection='Customers', query={}, projection={ "_id": 0},
                                output_file_path='/opt/airflow/dags/Datafile/Inputfile/Olist/Customers.csv')

with DAG('mongo_connector_extract_data', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    mongo_connector_extract_data = PythonOperator(
        task_id='mongo_connector_extract_data',
        python_callable=exec_olist_mongo_connector_extract_data
    )

    mongo_connector_extract_data
