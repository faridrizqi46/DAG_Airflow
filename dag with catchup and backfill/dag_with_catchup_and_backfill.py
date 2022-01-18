from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_arg = {
    'owner': 'farid',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'dag_with_catchup_and_backfill_v0',
    default_args = default_arg,
    start_date = datetime(2022, 1, 1),
    schedule_interval = '@daily',
    catchup=True # If True, airflow will run task from 2022-1-1 till today, if false only run today task
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task!"
    )
    
    task1
    
# Backfill dipakai untuk mengambil log task dari tanggal tertentu hingga tanggal tertentu
# Backfill hanya dapat dijalankan melalui console
# 1. ketik docker-compose ps , lalu copy container ID yang apache/airflow 8080/tcp
# 2. lalu pada console ketik "docker exec -it 'apache container id' bash"
# 3. lalu akan masuk ledalam bash dari si container id
# 4. lalu ketikan "airflow dags backfill -s 'tanggal awal cth:2022-1-1' -e 'tanggal akhir cth:2022-1-5' 'masukan dag_id cth :dag_with_catchup_and_backfill_v0 ' "
# 5. lalu ketik exit bila telah selesai, agar keluar dari bash container id
# 6. buka dag pada website