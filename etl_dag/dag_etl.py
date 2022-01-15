from datetime import datetime, timedelta
from etl_data2 import run_etl
from airflow import DAG
from airflow.operators.python import PythonOperator

default_arg = {
    'owner': 'farid',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'email' : ['first122448@gmail.com'],
}

with DAG(
    dag_id = 'dag_etl_v0',
    default_args = default_arg,
    start_date = datetime(2022, 1, 15),
    schedule_interval = '@daily',
) as dag: 
    task1 = PythonOperator(task_id = 'etl', python_callable=run_etl)
    
    task1
    