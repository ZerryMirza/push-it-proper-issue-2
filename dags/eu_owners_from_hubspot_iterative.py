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

target_time_zone = timezone('America/Chicago')
eu_hubspot_token = "pat-na1-7a04463c-7ff1-4ba9-9002-6b788df7b6d3"
s3_bucket_name = 'euredshiftbucket'
s3_key = 'required_columns_owners'
s3_aws_conn_id = 'S3_Connection_EU'
redshift_aws_conn_id = 'Redhsift_DW_EU'
target_table_name = 'hubspot_owners'

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.__str__()
    
# fetch properties and contacts
def fetch_all_owners():
    client = hubspot.Client.create(access_token=eu_hubspot_token)

    # Iteration and API parameters
    all_owners = []
    after=0
    iteration_no = 1
    limit=10
    
    # Loop until the offset is None...
    while True:
        # Print the Iteration No.
        print(iteration_no)

        # Make a GET request to the /crm/v3/objects/contacts endpoint with the query parameters
        if after:
            # Make a GET request to the /crm/v3/objects/contacts endpoint with the query parameters
            api_response = client.crm.owners.owners_api.get_page(limit=limit, archived=False, after=after)
        else:
            # Make a GET request to the /crm/v3/objects/contacts endpoint with the query parameters
            api_response = client.crm.owners.owners_api.get_page(limit=limit, archived=False)

        # Extract the contacts data from the response
        owners = api_response.results

        # Check if all contacts have been retrieved
        if not owners:
            break

        # Add result to all_contact list
        all_owners.extend(owners)

        # Increment the offset for the next request, if api_response is not None
        if api_response.paging and api_response.paging.next and api_response.paging.next.after:
            after = api_response.paging.next.after
        # Break the loop if API response is None
        else:
            break
        # Increment for next iteration
        iteration_no+=1

    print('All Owners Fetched')
    
    # convert the hubSpot returned object to a dictionary
    all_owners_dict = [obj.to_dict() for obj in all_owners]
    print("All Objects converted to Dictionary")
    print(f'all_owners_dict[0]={all_owners_dict[0]}')

    # Convert the response to json to store in s3
    json_data = json.dumps(all_owners_dict, default=datetime_converter)
    print('All Companies converted to Json')
    # print(f'json_data[0]={json_data[0]}')
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


# fetch data from s3 and transform to csv...
def fetch_owners_json_from_s3(**kwargs):
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)

    # Fetch the JSON file from S3...
    json_data = s3_hook.read_key(s3_key + ".json", s3_bucket_name)

    print('Data extracted successfully')

    # Deserialize the JSON data into a Python dictionary...
    json_dict = json.loads(json_data)
    print(f'First object of json dict = {json_dict[0]}')

    df = pd.DataFrame.from_dict(json_dict)
    # Data should be in the same sequence as the Redshift target table, as per the Copy statement Docx...
    df = df[[
        'id',
        'first_name',
        'last_name',
        'email',
        'created_at',
        'teams',
        'user_id'
        ]]
    # Convert Timezone for utc columns
    utc_datetime_cols = ['created_at']
    df[utc_datetime_cols] = df[utc_datetime_cols].apply(pd.to_datetime).apply(lambda x: x.dt.tz_convert(target_time_zone))
    print("Dataframe Head")
    print(df.head())

    csv_data = df.to_csv(index=False)
    print("Data converted to csv string")

    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key + ".txt",
        bucket_name=s3_bucket_name,
        replace=True
    )
    print("Data saved to csv successfully")


    # Store the JSON dictionary in an XCom variable for further processing
    # kwargs['ti'].xcom_push(key='json_data', value=json_dict)

default_args = {
    'owner': 'dell',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id = 'EU_Fetch_Owners_v1',
    description='Fetch Owners from HubSpot API',
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_pull_data_from_api = PythonOperator(
        task_id='Fetch_Owners',
        python_callable=fetch_all_owners
    )
    task_fetch_raw_data_from_s3 = PythonOperator(
        task_id='Fetch_raw_data_from_s3',
        python_callable=fetch_owners_json_from_s3
    )
    # create table if not exists...
    task_create_owner_table = RedshiftSQLOperator(
        task_id='create_table_if_not_exists',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS public.hubspot_owners (
            id BIGINT,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP,
            teams VARCHAR(255),
            user_id BIGINT
            );
        """
    )
    task_truncate_owner_table = RedshiftSQLOperator(
        task_id='truncate_owners_table',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            TRUNCATE TABLE public.hubspot_owners;
        """
    )
    task_transfer_processed_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_s3_csv_to_redshift",
        redshift_conn_id=redshift_aws_conn_id,
        aws_conn_id=s3_aws_conn_id,
        s3_bucket=s3_bucket_name,
        s3_key=s3_key + ".txt",
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
        'id',
        'first_name',
        'last_name',
        'email',
        'created_at',
        'teams',
        'user_id'
        ]
)



    task_pull_data_from_api >> task_fetch_raw_data_from_s3 >> task_create_owner_table >> task_truncate_owner_table >> task_transfer_processed_s3_to_redshift
