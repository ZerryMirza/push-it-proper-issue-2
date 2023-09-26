from airflow import DAG
from airflow.operators.python import PythonOperator
from hubspot.crm.contacts import ApiException
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
import hubspot
import json
import pandas as pd
from time import sleep
from pytz import timezone


eu_hubspot_token = "pat-na1-02fb8948-b2e7-477c-bd7a-af7fc7962e88"
bucket_name = 'htaredshiftbucket'
s3_aws_conn_id = 'S3_Connection_HTA'
redshift_aws_conn_id = 'Redhsift_DW_HTA'
s3_key_pipeline = 'required_columns_pipeline'
s3_key_stages = "required_columns_stages"
target_table_name = 'hubspot_pipelines'

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.__str__()
    
# Fetch Pipeline n Stages
def fetch_all_pipelines():
    client = hubspot.Client.create(access_token=eu_hubspot_token)

    try:
        # extract all pipeline endpoint
        api_response = client.crm.pipelines.pipelines_api.get_all(object_type="Deals")
    except ApiException as e:
        print("Exception when calling pipelines_api->get_all: %s\n" % e)

    print('All Pipelines Fetched')
    all_pipelines = api_response.results
    # Convert the hubSpot returned object to a dictionary
    all_pipelines_dict = [obj.to_dict() for obj in all_pipelines]
    print("All Objects converted to Dictionary")
    print(f'all_pipelines_dict[0]={all_pipelines_dict[0]}')
    # Normalize the json format
    all_stages = []
    for obj in all_pipelines_dict:
        if 'stages' in obj:
            pipeline_stage = obj.pop('stages')
            temp_dict = [{'id': t.get('id'), 
                          'label':t.get('label'), 
                          'pipeline_id': obj.get('id'), 
                          'display_order': t.get('display_order'), 
                          'metadata': t.get('metadata')
                          } for t in pipeline_stage]
            all_stages.extend(temp_dict)
    print('All Contacts Normalized to single attribute pattern')
    print(f'First normalized value: {all_pipelines_dict[0]}') 

    # Convert the Pipelines response to json to store in s3
    json_data = json.dumps(all_pipelines_dict, default=datetime_converter)
    # Convert the Stages response to json to store in s3
    json_data_stages = json.dumps(all_stages, default=datetime_converter)
    print('All Contacts converted to Json')
    # Build S3 Connection - Connection info in Airflow > Admin > Connections
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)
    print("Connection with s3 build successfully")

    
    retries = 3
    delay_sec = 30
    # Save the pipeline json file to S3
    for i in range(retries+1):
        try:
            s3_hook.load_string(json_data, 
                                key=s3_key_pipeline + '.json', 
                                bucket_name=bucket_name, 
                                replace=True)
            print(f'Pipeline File stored in S3')
            break
        except AirflowException as e:
            print(f"Failed to upload file to S3 (attempt {i + 1}): {str(e)}")
            if i < retries:
                print(f'Retrying after {delay_sec} seconds...')
                sleep(delay_sec)
            else:
                raise e
            
    # Save the Stages json file to S3
    for i in range(retries+1):
        try:
            s3_hook.load_string(json_data_stages, 
                                key=s3_key_stages + '.json', 
                                bucket_name=bucket_name, 
                                replace=True)
            print(f'Stages File stored in S3')
            break
        except AirflowException as e:
            print(f"Failed to upload file to S3 (attempt {i + 1}): {str(e)}")
            if i < retries:
                print(f'Retrying after {delay_sec} seconds...')
                sleep(delay_sec)
            else:
                raise e


# Fetch Deals data from s3 and transform to csv
def fetch_pipelines_json_from_s3(**kwargs):
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)

    # Fetch the JSON file from S3
    json_data = s3_hook.read_key(s3_key_pipeline + ".json", bucket_name)
    print('Data extracted successfully')

    # Deserialize the JSON data into a Python dictionary
    json_dict = json.loads(json_data)
    print(f'first object of json dict = {json_dict[0]}')

    df = pd.DataFrame.from_dict(json_dict)
    # Data should be in the same sequence as the Redshift target table, as per the Copy statement Docx...

    df = df[[
        'id',
        'label',
        'display_order'
        ]]
    print("Dataframe Head")
    print(df.head())

    csv_data = df.to_csv(index=False)
    print("Data converted to csv string")

    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key_pipeline + '.txt',
        bucket_name=bucket_name,
        replace=True
    )
    print("Data saved to csv successfully")

# Fetch Deal-Company data from s3 and transform to csv
def fetch_pipeline_stages_json_from_s3(**kwargs):
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)

    # Fetch the JSON file from S3
    json_data = s3_hook.read_key(s3_key_stages + ".json", bucket_name)
    print('Data extracted successfully')

    # Deserialize the JSON data into a Python dictionary
    json_dict = json.loads(json_data)
    print(f'first object of json dict = {json_dict[0]}')

    df = pd.DataFrame.from_dict(json_dict)
    # Data should be in the same sequence as the Redshift target table, as per the Copy statement Docx...

    df = df[[
        'id',
        'label',
        'pipeline_id',
        'display_order',
        'metadata'
        ]]
    print("Dataframe Head")
    print(df.head())

    csv_data = df.to_csv(index=False)
    print("Data converted to csv string")

    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key_stages + '.txt',
        bucket_name=bucket_name,
        replace=True
    )
    print("Data saved to csv successfully")


default_args = {
    'owner': 'dell',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id = 'HTA_Fetch_Pipeline_n_stages_v1',
    description='Fetch Contacts from HubSpot API',
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_1_pull_pipeline_n_stages_from_api = PythonOperator(
        task_id='1_Fetch_Pipelines_n_Stages_from_API',
        python_callable=fetch_all_pipelines
    )

    task_2a_fetch_pipeline_raw_data_from_s3 = PythonOperator(
        task_id='2a_Fetch_Pipelines_raw_data_from_s3',
        python_callable=fetch_pipelines_json_from_s3
    )
    
    task_2b_fetch_pipeline_stages_raw_data_from_s3 = PythonOperator(
        task_id='2b_Fetch_Stages_raw_data_from_s3',
        python_callable=fetch_pipeline_stages_json_from_s3
    )
    
    # Create pipelines table if not exists...
    task_3a_create_pipelines_table = RedshiftSQLOperator(
        task_id='3a_create_pipelines_table_if_not_exists',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS public.hubspot_pipelines (
            id VARCHAR(255),
            label VARCHAR(255),
            display_order INT
            );
        """
    )
    # Create stages table if not exists...
    task_3b_create_pipelines_stages_table = RedshiftSQLOperator(
        task_id='3b_create_pipelines_stages_table_if_not_exists',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS public.hubspot_pipeline_stages (
            id VARCHAR(255),
            label VARCHAR(255),
            pipeline_id VARCHAR(255),
            display_order INT,
            metadata VARCHAR(255)
            );
        """
    )

    task_4a_truncate_pipelines_table = RedshiftSQLOperator(
        task_id='4a_truncate_pipelines_table',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            TRUNCATE TABLE public.hubspot_pipelines;
        """
    )
    task_4b_truncate_pipeline_stages_table = RedshiftSQLOperator(
        task_id='4b_truncate_pipeline_stages_table',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            TRUNCATE TABLE public.hubspot_pipeline_stages;
        """
    )

    transfer_5a_pipelines_s3_to_redshift = S3ToRedshiftOperator(
        task_id="5a_transfer_pipelines_s3_csv_to_redshift",
        redshift_conn_id=redshift_aws_conn_id,
        aws_conn_id=s3_aws_conn_id,
        s3_bucket=bucket_name,
        s3_key=s3_key_pipeline + '.txt',
        # method='replace'
        schema="public",
        # delimeter=',',
        table=target_table_name,
        copy_options=["csv ", 
                    "IGNOREHEADER 1",
                    "TIMEFORMAT 'auto'"],
        column_list=[
        'id',
        'label',
        'display_order'
        ]
    )
    transfer_5b_pipeline_stages_s3_to_redshift = S3ToRedshiftOperator(
        task_id="5b_transfer_pipeline_stages_s3_csv_to_redshift",
        redshift_conn_id=redshift_aws_conn_id,
        aws_conn_id=s3_aws_conn_id,
        s3_bucket=bucket_name,
        s3_key=s3_key_stages + '.txt',
        # method='replace'
        schema="public",
        # delimeter=',',
        table='hubspot_pipeline_stages',
        copy_options=["csv ", 
                    "IGNOREHEADER 1",
                    "TIMEFORMAT 'auto'"],
        column_list=[
        'id',
        'label',
        'pipeline_id',
        'display_order',
        'metadata'
        ]
    )



    task_1_pull_pipeline_n_stages_from_api >> task_2a_fetch_pipeline_raw_data_from_s3 >> task_3a_create_pipelines_table >> task_4a_truncate_pipelines_table >> transfer_5a_pipelines_s3_to_redshift
    task_1_pull_pipeline_n_stages_from_api >> task_2b_fetch_pipeline_stages_raw_data_from_s3 >> task_3b_create_pipelines_stages_table >> task_4b_truncate_pipeline_stages_table >> transfer_5b_pipeline_stages_s3_to_redshift
   