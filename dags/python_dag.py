from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='Task_1_ID', key='first_name')
    last_name = ti.xcom_pull(task_ids='Task_1_ID', key='last_name')
    age = ti.xcom_pull(task_ids='Task_2_ID', key='age')
    print(f'Hi there, my name is {first_name} {last_name} and I am {age} years old')

def get_name(ti):
    ti.xcom_push(key='first_name', value='Zerry')
    ti.xcom_push(key='last_name', value='Mirza')

def get_age(ti):
    ti.xcom_push(key='age', value=27)

with DAG(
    dag_id='Dag_with_python_operator_v4',
    description='Our first Dag using python Operator',
    default_args=default_args,
    start_date=datetime(2023,3,30),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='Task_1_ID',
        python_callable=get_name
    )

    task2 = PythonOperator(
        task_id='Task_2_ID',
        python_callable=get_age
    )
    task3 = PythonOperator(
        task_id='Task_3_ID',
        python_callable=greet
    )

    [task1,task2] >> task3