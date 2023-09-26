from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args ={
    'owner': 'dell',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='Dag_with_Decorator_v1', 
    description='Dag_with_Description', 
    default_args=default_args, 
    start_date=datetime(2023,3,30), 
    schedule_interval='@daily')
def hello_world_with_dag():

    @task(multiple_outputs=True)
    def get_name():
        return {"first_name": "Jerry", "last_name": "Mirza"}

    @task()
    def get_age():
        return 19

    @task()
    def greet(first_name, last_name, age):
        print(f'My name is {first_name} {last_name} and I am {age} years old')

    
    name = get_name()
    first_name = name['first_name']
    last_name = name['last_name']
    age = get_age()
    greet(first_name=first_name, last_name=last_name, age=age)


greet_dag = hello_world_with_dag()

