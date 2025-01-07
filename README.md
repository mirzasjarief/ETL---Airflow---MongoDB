## ETL Pipeline Automation on US Store Sales Report
## Description
Developing  an automated data pipeline system (ETL) by combining Apache Airflow for orchestration, PySpark for data transformation, and loading  transformed data into a MongoDB database.  This process is scheduled to run every Saturday between 09:10 and 09:30 with a 10-minute interval. Followed by data validation using Great Expectations.

## Prerequisites
- Python 3.x
- Apache Airflow
- Apache PySpark
- Python Great Expectation

## Project Structure

#### 1. Airflow installation 

#### 2. Configure DAG:

- Adjust the parameters in the dags.py  including the path for the extraction, transformation, and loading scripts.

#### 3. Run Airflow:

- Start the Airflow scheduler and web server with the command:
  
  airflow scheduler
  airflow webserver

#### 4. Access Airflow UI:

- Open a browser and access the Airflow UI at http://localhost:8080 to monitor and manage the DAG.

#### 5. Schedule DAG:

- The DAG will run automatically according to the specified schedule.
- Once the DAG is scheduled, the ETL process will run automatically
- After the ETL process is complete, US Store Sales Report data will be available in the target system specified in the load.py script.

#### 6. Data Validation:

- Installing Great Expectation
- Set up expectation
- Generate data and make sure all the data pass the validating prerequisites

