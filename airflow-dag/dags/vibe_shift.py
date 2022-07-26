from airflow import DAG
from airflow.decorators import dag, task
from main import run
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'schedule_interval':'@once',
    'start_date': datetime.utcnow(),
    'catchup': False
} 

with DAG(
    'vibe_shift',
    description='An automation of the vibe shift twitter sentiment program',
    default_args=default_args
) as dag:

    #Python task that calls our print_hello function
    run_main = PythonOperator(
        task_id='print_hello',
        python_callable=run()
    )
    all_done = EmptyOperator(task_id='all_done')
    #establish our task order
    run_main >> all_done