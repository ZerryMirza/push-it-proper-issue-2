from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'dell',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = "dag_with_back_fill_id_v1",
    description = "dag description for back fill",
    default_args=default_args,
    start_date = datetime(2023, 3, 30),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'Bash_task_1',
        bash_command = "echo This is a Bash Operator call"

    )

    task1