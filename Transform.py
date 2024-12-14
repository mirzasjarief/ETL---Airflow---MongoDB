'''
=================================================


Name  : Mirza Rendra Sjarief
Project : Airflow Transform

This program automates the data transformation function in the context of Airflow,
performing data transformation on the raw dataset and 
returning data in a cleaned condition after undergoing a data cleaning process. 

=================================================
'''

# install the necessary libraries 
from pyspark.sql.functions import col, sum, concat, lit, when, to_date, to_timestamp, upper, lower, date_format,concat_ws,hash,abs,monotonically_increasing_id
from pyspark.sql import SparkSession

# configure and create a new SparkSession instance.
spark = SparkSession.builder.getOrCreate()
    
#Loading dataset to PySpark
sales = spark.read.csv('/Users/mac/airflow/dags/P2M3_Mirza_Sjarief_data_raw.csv', header=True, inferSchema=True)
    
    
def transform_data(sales):
    '''
    The function transform_data(sales) is designed to process and transform 
    raw data (in this case, sales data). Transformation is a common step in an ETL pipeline 
    
    Data Cleaning: Removing duplicates, handling missing values, or correcting formatting issues.
    Feature Engineering: Creating new features, such as calculating totals or generating new columns (e.g., profit margin).
    Data Aggregation: Summarizing data by grouping (e.g., grouping by date, product category, or region).
    Filtering Data: Keeping only relevant rows or columns (e.g., sales above a threshold).
    
    return : The output would typically be a transformed version of the input sales data.
    '''
    
    
    #Compute code for replacing negative value with 0 in pyspark
    sales = sales.withColumn('Inventory', when(col('Inventory') < 0 , 0 ).otherwise(col('Inventory')))
    
    #Computing convert object to timestamp
    sales = sales.withColumn('Date', to_timestamp('Date','dd/MM/yy HH:mm:ss'))
    
    #Computing convert timestamp to date
    sales = sales.withColumn("Date", to_date("Date", "yyyy-MM-dd"))
    
    #Computing column drop
    sales = sales.drop('Area Code','Product','Margin','Profit')
    
    #Convert capital to lower case
    sales = (
    sales
    .withColumn('State', lower(col('State')))
    .withColumn('Market', lower(col('Market')))
    .withColumn('Market Size', lower(col('Market Size')))
    .withColumn('Product Type', lower(col('Product Type')))
    .withColumn('Type', lower(col('Type'))))
    
    #Converting column title from mix to lower case
    sales = (
    sales
    .withColumnRenamed("State", "state")
    .withColumnRenamed("Market", "market")
    .withColumnRenamed("Market Size", "market_size")
    .withColumnRenamed("Sales", "sales")
    .withColumnRenamed("COGS", "cogs")
    .withColumnRenamed("Total Expenses", "total_expenses")
    .withColumnRenamed("Marketing", "marketing")
    .withColumnRenamed("Inventory", "inventory")
    .withColumnRenamed("Budget Profit", "budget_profit")
    .withColumnRenamed("Budget COGS", "budget_cogs")
    .withColumnRenamed("Budget Margin", "budget_margin")
    .withColumnRenamed("Budget Sales", "budget_sales")
    .withColumnRenamed("ProductId", "product_id")
    .withColumnRenamed("Date", "date")
    .withColumnRenamed("Product Type", "product_type")
    .withColumnRenamed("Type", "type"))
    
    #Computing new unique value column
    sales = sales.withColumn (
    'purchased_id', abs(hash(concat_ws('',col('sales').cast('string'),col('cogs').cast('string'),col('inventory').cast('string'),monotonically_increasing_id().cast('string') ))))
    
    #Convert to csv
    sales.toPandas().to_csv('/Users/mac/airflow/dags/P2M3_Mirza_Sjarief_data_clean.csv')
    
    return sales

if __name__ == '__main__':
    
    transform_data(sales)
