from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import pyodbc


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_data():
    sql_server_conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-A2KHOCB\dell;DATABASE=AdventureWorksDW2019;Trusted_Connection=yes")
    # source_cursor = sql_server_conn.cursor()
    # source_cursor.execute('SELECT TOP (100) * FROM DimCustomer')
    df = pd.read_sql_query('SELECT TOP (100) * FROM DimCustomer', sql_server_conn)
    # sql_server_conn.close()
    print('Data loaded successfully')


with DAG(
    dag_id='SQL_Server_to_Redshift_v11',
    default_args=default_args,
    description='without s3',
    start_date=datetime(2023,4,2),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='1_Read_from_SQL_Server',
        python_callable=extract_data
    )

    task2 = BashOperator(
        task_id='2_Bash_Operator',
        bash_command='echo Data read successful'
    )

    task1 >> task2