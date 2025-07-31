import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from src.bronze_layer import extract_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG('data_pipeline_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    bronze_task = PythonOperator(
        task_id='bronze_layer',
        python_callable=extract_data
    )