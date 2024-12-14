'''
=================================================

Nama  : Mirza Rendra Sjarief
Project : Airflow (Extract)


This program operates the extract function in the context of Airflow, 
extracting the initial raw file from the specified 
path and transferring the data to the same path with a new file name
=================================================
'''

# import the necessary libraries 
from pyspark.sql import SparkSession

def load_data():
    '''
    In the context of Airflow, the extract_data function or task typically refers 
    to the process of extracting data from a source, such as a database, API, local file, 
    or cloud-based data. This function is usually designed to retrieve raw data so that it 
    can be used in subsequent steps (such as transformation or analysis).

    Returns: the extracted sales data.
        
    '''
    # configure and create a new SparkSession instance.
    spark = SparkSession.builder.getOrCreate()
    
    #Loading dataset to PySpark
    sales = spark.read.csv('/Users/mac/airflow/dags/us_store_sales.csv', header=True, inferSchema=True)
    
    # Coonvert dataframe to csv
    sales.toPandas().to_csv('/Users/mac/airflow/dags/P2M3_Mirza_Sjarief_data_raw.csv')
    
    return sales

if __name__ == '__main__':
    
    load_data()

