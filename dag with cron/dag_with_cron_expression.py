from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_arg = {
    'owner': 'farid',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'dag_with_cron_expression_v3',
    default_args = default_arg,
    start_date = datetime(2022, 1, 1),
    schedule_interval = '0 4 * * Mon',# ini adalah crontab, bisa lihat generate waktu di crontab.guru,bila seperti ini maka schedulenya setiap hari senin jam 4
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task!"
    )
    
    task1