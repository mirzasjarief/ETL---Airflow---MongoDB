'''
=================================================

Name : Mirza Rendra Sjarief
Project : Airflow (Load)

This program operates the load function in the context of Airflow, 
which automatically loads datasetsthat have undergone the data transformation 
process into MongoDB Compass.

=================================================
'''

# install the necessary libraries 
import pandas as pd
from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient



def load_data():
    '''
    This function connects to a MongoDB database and loads cleaned data from a CSV file into a 
    specified collection within that database.
    
    1. Establishing a connection to MongoDB using the URI.
    2. Selecting the target database and collection.
    3. Iterating through the DataFrame rows and inserting them into the MongoDB collection.
    '''
    DB_NAME ="ETL_us_store_sales"
    COLLECTION_NAME ="us_store_sales"
    uri = 'mongodb+srv://mirzarendra:mirzarendra@cluster0.fiwld.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
    data = pd.read_csv('/Users/mac/airflow/dags/P2M3_Mirza_Sjarief_data_clean.csv')

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    record = data.to_dict(orient='records')

    # Send a ping to confirm a successful connection
    try:
        inserted = collection.insert_many(record)
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        
    except Exception as e:
        print(e)
        
if __name__ == '__main__':
    load_data()