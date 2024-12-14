'''
=================================================

Name  : Mirza Rendra Sjarief
Project : Airflow DAG

This program operates the DAG (Directed Acyclic Graph) function 
in the context of Apache Airflow, whose main function and task 
are to serve as a framework for designing, scheduling, and executing 
sequences of tasks automatically.
=================================================
'''


# import the necessary libraries 
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    ''' 
    provides default configurations for tasks within a DAG. 
    These defaults help streamline task definitions by applying common settings across multiple tasks.
    It includes parameters like:

    owner: Identifies the owner of the tasks/DAG.
    start_date: Specifies when the DAG or tasks should start running.
    retries: Sets the number of retry attempts for failed tasks.
    retry_delay: Defines the waiting time between retry attempts.
    '''
    
    'owner': 'mirzasjarief',
    'start_date': dt.datetime(2024, 11, 1) - timedelta(hours=7), #specifies November 1, 2024
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    }

with DAG (
    '''
    In the context of workflow (Apache Airflow),
    a DAG represents a collection of tasks that need to be executed in a specific order 
    based on their dependencies.

    ETL_us_store_sales: The DAG's unique ID.
    description: A short text describing the purpose of the DAG (ETL for US Store Sales data).
    schedule_interval: A cron expression that schedules the DAG 
    default_args: These are default configurations (e.g., retries, owner) applied to tasks in the DAG.
    catchup=False: Prevents backfilling, meaning it won't run for past missed intervals.
    '''
    "ETL_us_store_sales",
    description='ETL process for Us Store Sales dataset',
    schedule_interval='10-30/10 9 * * 6',
    default_args=default_args, 
    catchup=False ) as dag:

    
    #Defining three tasks using the BashOperator in Apache Airflow
    extract_data = BashOperator(task_id='extract_data',
                            bash_command='python3 /Users/mac/airflow/dags/extract.py')
    transform_data = BashOperator(task_id='transform_data',
                            bash_command='python3 /Users/mac/airflow/dags/transform.py')
    load_data = BashOperator(task_id='load_data',
                            bash_command='python3 /Users/mac/airflow/dags/load.py')
    '''
    This line specifies the task dependencies (also called the execution order). 
    The >> operator means that extract_data must run before transform_data, 
    and transform_data must run before load_data. In other words, 
    the tasks will execute in this sequence:
    Extract data
    Transform the data
    Load the data
    This order ensures that the tasks run in the proper sequence according to the workflow.
    '''
    extract_data>>transform_data>>load_data