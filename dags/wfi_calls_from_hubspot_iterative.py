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
hta_hubspot_token = "pat-na1-047e1c65-3481-4d3e-afc3-57558aa07b8b"
s3_bucket_name = 'wfiredshiftbucket'
s3_aws_conn_id = 'S3_Connection_WFI'
redshift_aws_conn_id = 'Redhsift_DW_WFI'

s3_key = 'required_columns_calls'
target_table_name = 'hubspot_calls'

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.__str__()
    
# fetch properties and contacts
def fetch_all_calls():
    client = hubspot.Client.create(access_token=hta_hubspot_token)
    # try:
    #     contact_api_response = client.crm.properties.core_api.get_all(object_type="CALLS")
    #     # pprint(contact_api_response)
    # except ApiException as e:
    #     print("Exception when calling basic_api->get_by_id: %s\n" % e)
    #     pass
    # contact_property_dictionary = dict()
    # for i in contact_api_response.results:
    #     contact_property_dictionary.update({i.name:i.label})
    # print(contact_property_dictionary)
    # Main Properties...
    call_property_dictionary = {'hs_activity_type': 'Activity Type',
                                'hs_call_direction': 'Call Direction',
                                'hs_call_duration': 'Call Duration',
                                'hs_call_source': 'Call Source',
                                'hs_call_status': 'Call Status',
                                'hs_createdate': 'CreateDate',
                                'hs_call_title': 'Call Title',
                                'hs_lastmodifieddate': 'Last Modified',
                                'hubspot_owner_id': 'Hubspot Owner ID'
                                }

    # Iteration and API parameters
    all_calls = []
    after=0
    iteration_no = 1
    limit=100
    
    # Loop until the offset is None...
    while True:
        # Print the Iteration No.
        print(iteration_no)

        # Make a GET request to the /crm/v3/objects/calls endpoint with the query parameters
        api_response = client.crm.objects.calls.basic_api.get_page(limit=limit, archived=False, after=after,
                                                               properties=list(call_property_dictionary.keys()) )

        # Extract the calls data from the response
        calls = api_response.results

        # Check if all calls have been retrieved
        if not calls:
            break

        # Add result to all_calls list
        all_calls.extend(calls)

        # Increment the offset for the next request, if api_response is not None
        if api_response.paging and api_response.paging.next and api_response.paging.next.after:
            after = api_response.paging.next.after
        # Break the loop if API response is None
        else:
            break
        # Increment for next iteration
        iteration_no+=1

    print('All Calls Fetched')
    
    # convert the hubSpot returned object to a dictionary
    all_calls_dict = [obj.to_dict() for obj in all_calls]
    print("All Objects converted to Dictionary")
    print(f'all_calls_dict[0]={all_calls_dict[0]}')
    # normalize the json format
    for obj in all_calls_dict:
        obj.update(obj.pop('properties'))
    print('All Calls Normalized to single attribute pattern')
    print(f'First normalized value: {all_calls_dict[0]}')

    # Convert the response to json to store in s3
    json_data = json.dumps(all_calls_dict, default=datetime_converter)
    print('All Calls converted to Json')
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
        'id',
        'hs_activity_type',
        'hs_call_direction',
        'hs_call_duration',
        'hs_call_source',
        'hs_call_status',
        'hs_createdate',
        'hs_call_title',
        'hs_lastmodifieddate',
        'hubspot_owner_id'
        ]]
    # Convert Timezone for utc columns
    utc_datetime_cols = ['hs_createdate', 'hs_lastmodifieddate']
    df[utc_datetime_cols] = df[utc_datetime_cols].apply(pd.to_datetime).apply(lambda x: x.dt.tz_convert(target_time_zone))

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
    dag_id = 'WFI_Fetch_Calls_min_properties_v1',
    description='Fetch Calls from HubSpot API',
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_pull_calls_from_api = PythonOperator(
        task_id='Fetch_Calls_and_Properties',
        python_callable=fetch_all_calls
    )
    task_fetch_raw_data_from_s3 = PythonOperator(
        task_id='Fetch_raw_data_from_s3',
        python_callable=fetch_json_from_s3
    )
    # create table if not exists...
    task_create_calls_table = RedshiftSQLOperator(
        task_id='create_table_if_not_exists',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS public.hubspot_calls (
            id BIGINT,
            hs_activity_type VARCHAR(255),
            hs_call_direction VARCHAR(255),            
            hs_call_duration BIGINT,
            hs_call_source VARCHAR(255),
            hs_call_status VARCHAR(255),
            hs_call_title VARCHAR(255),
            hs_lead_status VARCHAR(255),
            hs_createdate TIMESTAMP,
            hs_lastmodifieddate TIMESTAMP,
           
            hubspot_owner_id BIGINT
            );
        """
    )
    task_truncate_calls_table = RedshiftSQLOperator(
        task_id='truncate_company_table',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            TRUNCATE TABLE public.hubspot_calls;
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
        'id',
        'hs_activity_type',
        'hs_call_direction',
        'hs_call_duration',
        'hs_call_source',
        'hs_call_status',
        'hs_createdate',
        'hs_call_title',
        'hs_lastmodifieddate',
        'hubspot_owner_id'
        ]
)



    task_pull_calls_from_api >> task_fetch_raw_data_from_s3 >> task_create_calls_table >> task_truncate_calls_table >> transfer_s3_to_redshift
