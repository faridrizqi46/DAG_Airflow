from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_arg = {
    'owner': 'farid',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def get_name(ti):
    ti.xcom_push(key='first_name', value='Farid')
    ti.xcom_push(key='last_name', value='Rizqi')
    
def get_age(ti):
    ti.xcom_push(key='age', value=22)
    
def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name') # Get the return values from get_name
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f'Hello World {first_name} {age} {last_name}')

with DAG(
    dag_id = 'dag_python_operatorv1',
    default_args = default_arg,
    description = 'This is our first dag using python operator',
    start_date = datetime(2022, 1, 13, 2),
    schedule_interval = '@daily'
) as dag :
    task1 = PythonOperator(task_id='greet', python_callable=greet)
    task2 = PythonOperator(task_id='get_name', python_callable=get_name)
    task3 = PythonOperator(task_id='get_age', python_callable=get_age)
    
    [task2, task3] >> task1
    