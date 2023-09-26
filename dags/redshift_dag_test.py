from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dell',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='redshift_dag_v2',
    description='Redshift Dag here',
    start_date=datetime(2023,4,1),
    schedule_interval='@daily'
) as dag:
    # update existing table first
    task1 = RedshiftSQLOperator(
        task_id='1_Create_table',
        redshift_conn_id='Redhsift_DW_EdTech',
        sql="""
            Create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    task2 = RedshiftSQLOperator(
        task_id='2_Insert_into_table',
        redshift_conn_id='Redhsift_DW_EdTech',
        # using airflow macros here...
        sql="""
            insert into dag_runs(dt, dag_id) VALUES ('{{ds}}', '{{dag.dag_id}}')
        """
    )

    task1 >> task2