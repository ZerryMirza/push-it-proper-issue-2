from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


default_args = {
    'owner': 'dell'
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}

etl_dag = DAG(
    dag_id="Load_from_SQL_S3_Redshift_V1",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
)

s3_file_name = 'action-test_1.csv'
bucket_name = 'zubairtests3ci'
s3_aws_conn_id = 'S3_Connection_ED_Tech'
target_table_name = 'action_table_2'

# Load data from Source to S3
sql_to_s3_task = SqlToS3Operator(
    task_id="sql_to_s3_task",
    aws_conn_id=s3_aws_conn_id,
    sql_conn_id='Redhsift_DW_EdTech',
    query='select action_date, amount, mins, calls from action_table_1',
    s3_bucket=bucket_name,
    s3_key=s3_file_name,
    file_format='csv',
    pd_kwargs={'index':False},
    replace=True,
    dag=etl_dag
)

# Transform the data
# Load from S3 and save back to S3
transform_s3_data = 'abc'


transfer_s3_to_redshift = S3ToRedshiftOperator(
    task_id="transfer_s3_to_redshift",
    redshift_conn_id='Redhsift_DW_EdTech',
    aws_conn_id=s3_aws_conn_id,
    s3_bucket=bucket_name,
    s3_key=s3_file_name,
    # method='replace'
    schema="public",
    # delimeter=',',
    # what if the table already exists?
    table=target_table_name,
    # how the records are processed? Row-wise insert / batch?
    # Can't we use the copy command directly that is supported in Redshift? 
    copy_options=["csv ", 
                  "IGNOREHEADER 1"],
    column_list=[
        ('action_date'),
        ('amount'),
        ('mins'),
        ('calls')
    ],
    dag=etl_dag
)

sql_to_s3_task >> transfer_s3_to_redshift