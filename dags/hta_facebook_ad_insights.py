from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
import json 
import pandas as pd
from time import sleep
from pytz import timezone
import requests

target_time_zone = timezone('America/Chicago')
access_token = 'EAADrejEsIrYBO7Qm9zSxyHRDCsZBDSGRThKF9BWsZBkmtyjh7xDbEJyMUZCkdtcksfrJdDlEDi2D0fmBj2cT03VwxgwlaBkDQP0MSRqVoLpo4fIUobeVcIQavsZCM45OFZCntp2t4oKbuZAY7xrMCdNjcU03PjY9TvMB5oMHfVVSybgcQenI9lk7uw'
facebook_account_id = '2645330488931717'
s3_bucket_name = 'htaredshiftbucket'
s3_aws_conn_id = 'S3_Connection_HTA'
redshift_aws_conn_id = 'Redhsift_DW_HTA'

s3_key = 'facebook_ad_insights'
target_table_name = 'facebook_ad_insights'

# Facebook returns the date of last 37 months
from_date = date.today() + relativedelta(months=-37)
from_date = from_date.strftime('%Y-%m-%d')
to_date = date.today().strftime('%Y-%m-%d')

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.__str__()
    
# fetch properties and contacts
def fetch_all_insights():
    
    # Iteration and API parameters
    iteration_no = 1

    
    url = f'https://graph.facebook.com/v17.0/act_'+facebook_account_id+'/insights'
    params = {
        'access_token': access_token,
        'fields': 'date_start,date_stop,spend, impressions, account_name',
        'time_increment': 1,
        'time_range':'{"since":"'+from_date+'","until":"'+to_date+'"}',
    #    'date_presets': 'maximum',
        'after':'',
    #    'breakdown': 'campaign_name',
        'limit':100
    }
    all_insights = []
    
    # Loop until the offset is None...
    while True:
        print(iteration_no)
        response = requests.get(url, params=params)
        # Parse the response and extract spend data
        data = response.json()
        if data.get('data'):
            all_insights.extend(data['data'])
        else:
            break
        iteration_no = iteration_no + 1
        if data.get('paging'):
            if data.get('paging').get('cursors'):
                if data.get('paging').get('cursors').get('after'):
                    params['after'] = data.get('paging').get('cursors').get('after')
        else:
            break

    print('All Contacts Fetched')
    
    # convert the hubSpot returned object to a dictionary
    
    # Convert the response to json to store in s3
    json_data = json.dumps(all_insights, default=datetime_converter)
    print('All insights converted to Json')

    # Build S3 Connection - Connection info in Airflow > Admin > Connections
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)
    print("Connection with s3 build successfully")

    # S3 bucket and file info
    retries = 3
    delay_sec = 30
    # Save the json file to S3
    for i in range(retries+1):
        try:
            s3_hook.load_string(json_data, 
                                key=s3_key + ".json", 
                                bucket_name=s3_bucket_name, 
                                replace=True)
            print(f'File stored in S3')
            break
        except AirflowException as e:
            print(f"Failed to upload file to S3 (attempt {i + 1}): {str(e)}")
            if i < retries:
                print(f'Retrying after {delay_sec} seconds...')
                sleep(delay_sec)
            else:
                raise e


# fetch data from s3 and transform to csv
def fetch_json_from_s3(**kwargs):
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)

    # Fetch the JSON file from S3
    json_data = s3_hook.read_key(s3_key + ".json", s3_bucket_name)

    print('Data extracted successfully')

    # Deserialize the JSON data into a Python dictionary
    json_dict = json.loads(json_data)
    print(f'first object of json dict = {json_dict[0]}')

    df = pd.DataFrame.from_dict(json_dict)
    # Data should be in the same sequence as the Redshift target table, as per the Copy statement Docx...
    df = df[[
        'date_start',
        'date_stop',
        'spend',
        'impressions',
        'account_name',
        ]]
    
    print("Dataframe Head")
    print(df.head())

    csv_data = df.to_csv(index=False)
    print("Data converted to csv string")

    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key + '.txt',
        bucket_name=s3_bucket_name,
        replace=True
    )
    print("Data saved to csv successfully")



default_args = {
    'owner': 'dell',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id = 'HTA_Fetch_Facebook_Insights',
    description='Fetch Insights from Facebook',
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_pull_contacts_from_api = PythonOperator(
        task_id='Fetch_Contacts_and_Properties',
        python_callable=fetch_all_insights
    )
    task_fetch_raw_data_from_s3 = PythonOperator(
        task_id='Fetch_raw_data_from_s3',
        python_callable=fetch_json_from_s3
    )
    # create table if not exists...
    task_create_contacts_table = RedshiftSQLOperator(
        task_id='create_table_if_not_exists',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS public.facebook_ad_insights (
            date_start TIMESTAMP,
            date_stop TIMESTAMP,
            spend NUMERIC(10,2),
            impressions NUMERIC(10,2),
            account_name VARCHAR(255)
            );
        """
    )
    task_truncate_contacts_table = RedshiftSQLOperator(
        task_id='truncate_facebook_ad_insights_table',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            TRUNCATE TABLE public.facebook_ad_insights;
        """
    )
    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_s3_csv_to_redshift",
        redshift_conn_id=redshift_aws_conn_id,
        aws_conn_id=s3_aws_conn_id,
        s3_bucket=s3_bucket_name,
        s3_key=s3_key + '.txt',
        # method='replace'
        schema="public",
        # delimeter=',',
        # what if the table already exists?
        table=target_table_name,
        # how the records are processed? Row-wise insert / batch?
        # Can't we use the copy command directly that is supported in Redshift? 
        copy_options=["csv ", 
                    "IGNOREHEADER 1",
                    "TIMEFORMAT 'auto'"],
        column_list=[
        'date_start',
        'date_stop',
        'spend',
        'impressions',
        'account_name',
        ]
)



    task_pull_contacts_from_api >> task_fetch_raw_data_from_s3 >> task_create_contacts_table >> task_truncate_contacts_table >> transfer_s3_to_redshift
