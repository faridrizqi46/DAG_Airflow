from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_arg = {
    'owner': 'farid',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

@dag(dag_id = 'dag_taskflowAPI',
    default_args = default_arg,
    start_date = datetime(2022, 1, 13, 2),
    schedule_interval = '@daily'
    )
def hello_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {'first_name': 'Farid', 'last_name': 'Rizqi'}
    
    @task()
    def get_age():
        return 22
    
    @task()
    def greet(first_name, last_name, age):
        print(f'Hello World {first_name} {last_name} {age}')
    
    name_dict = get_name()
    age = get_age()
    greet(first_name= name_dict['first_name'],
          last_name= name_dict['last_name'],
          age=age)

greet_dag = hello_world_etl()