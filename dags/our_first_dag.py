from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='our_first_dag_v3',
    default_args=default_args,
    description='Our First Dag Description',
    start_date=datetime(2023, 3, 27, 2),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo I am the second task and Ill be running after task one"
        )
    task3 = BashOperator(
        task_id="third_task",
        bash_command="echo I am the third task and Ill be running after task one"
    )
    
    # task1.set_downstream([task2, task3])
    # task1.set_downstream(task3)
    task1 >> [task2, task3]