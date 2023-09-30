from airflow import settings
from airflow.models import Connection

# Create a new Connection object
conn = Connection(
    conn_id='my_mongodb',   #Input connector info here!
    conn_type='mongo',      #Input connector info here!
    host='172.17.0.2',      #Input connector info here!
    schema=Olist,           #Input connector info here!
    port=27017,             #Input connector info here!
    extra=None              #Input connector info here!
)

# Add the MongoDB connection to Airflow's settings
session = settings.Session()
session.add(conn)
session.commit()