from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import io
from datetime import datetime, timedelta

default_args = {
    'owner': 'dell',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def csv_transformer():
    s3_hook = S3Hook(aws_conn_id='S3_Connection')
    bucket_name = 'sqls3toredshift'
    input_key = 'action-test_1.csv'
    output_key = 'action-test_transformed.csv'
    csv_data = s3_hook.read_key(key=input_key, bucket_name=bucket_name)
    # Print the csv_data to console.
    # csv_buffer = io.StringIO(csv_data)
    # print(f'CSV Data Variable: {csv_buffer}')
    df = pd.read_csv(io.StringIO(csv_data))
    print(f'df before column change {df}')
    df.rename(columns={'action_date': 'Date_of_Action'}, inplace=True)
    # transform the data...
    # ... 
    # ...
    # print(f'After Transformation Csv Buffer: {csv_buffer}')
    # print(f'df after column change {df}')
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
   
    # print(f'Buffer After to CSC: {csv_buffer}')
    # print(f'Sting IO Values: {}')
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=output_key,
        bucket_name=bucket_name,
        replace=True
    )


with DAG(
    dag_id = 's3_transformer_v1',
    description='S3 Transformer',
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    Task1 = PythonOperator(
        task_id = '1_Transform',
        python_callable=csv_transformer
    )

    Task1
