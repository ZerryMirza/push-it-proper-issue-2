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
hta_hubspot_token = "pat-na1-4966b9f9-564b-4db8-80eb-c8396e52e870"
s3_bucket_name = 'plredshiftbucket'
s3_aws_conn_id = 'S3_Connection_PL'
redshift_aws_conn_id = 'Redhsift_DW_PL'

s3_key = 'required_columns_owners'
target_table_name = 'hubspot_companies'

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.__str__()
    
# fetch properties and contacts
def fetch_all_companies():
    client = hubspot.Client.create(access_token=hta_hubspot_token)
    # try:
    #     contact_api_response = client.crm.properties.core_api.get_all(object_type="CONTACTS")
    #     # pprint(contact_api_response)
    # except ApiException as e:
    #     print("Exception when calling basic_api->get_by_id: %s\n" % e)
    #     pass
    # contact_property_dictionary = dict()
    # for i in contact_api_response.results:
    #     contact_property_dictionary.update({i.name:i.label})
    # print(contact_property_dictionary)
    company_property_dictionary = {
        'name': 'Company name',
        'hubspot_owner_id': 'Company owner',
        'createdate': 'Create Date',
        'ownername': 'HubSpot Owner Name',
        }

    # Iteration and API parameters
    all_companies = []
    after=0
    iteration_no = 1
    limit=100
    
    # Loop until the offset is None...
    while True:
        # Print the Iteration No.
        print(iteration_no)

        # Make a GET request to the /crm/v3/objects/contacts endpoint with the query parameters
        api_response = client.crm.companies.basic_api.get_page(limit=limit, archived=False, after=after, properties=list(company_property_dictionary.keys()))

        # Extract the contacts data from the response
        companies = api_response.results

        # Check if all contacts have been retrieved
        if not companies:
            break

        # Add result to all_contact list
        all_companies.extend(companies)

        # Increment the offset for the next request, if api_response is not None
        if api_response.paging and api_response.paging.next and api_response.paging.next.after:
            after = api_response.paging.next.after
        # Break the loop if API response is None
        else:
            break
        # Increment for next iteration
        iteration_no+=1

    print('All Companies Fetched')
    
    # convert the hubSpot returned object to a dictionary
    all_companies_dict = [obj.to_dict() for obj in all_companies]
    print("All Objects converted to Dictionary")
    print(f'all_companies_dict[0]={all_companies_dict[0]}')
    # normalize the json format
    for obj in all_companies_dict:
        obj.update(obj.pop('properties'))
    print('All Companies Normalized to single attribute pattern')
    print(f'First normalized value: {all_companies_dict[0]}')

    # Convert the response to json to store in s3
    json_data = json.dumps(all_companies_dict, default=datetime_converter)
    print('All Companies converted to Json')
    # print(f'json_data[0]={json_data[0]}')
    # Build S3 Connection - Connection info in Airflow > Admin > Connections
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)
    print("Connection with s3 build successfully")

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
def fetch_company_json_from_s3(**kwargs):
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)
    # Fetch the JSON file from S3
    json_data = s3_hook.read_key(s3_key + ".json", s3_bucket_name)

    print('Data extracted successfully')

    # Deserialize the JSON data into a Python dictionary
    json_dict = json.loads(json_data)
    print(f'First object of json dict = {json_dict[0]}')

    df = pd.DataFrame.from_dict(json_dict)
    # Data should be in the same sequence as the Redshift target table, as per the Copy statement Docx...
    df = df[[
        'id',
        'name',
        'hubspot_owner_id',
        'createdate',
        'ownername'
        ]]
    # Convert Timezone for utc columns
    utc_datetime_cols = ['createdate']
    df[utc_datetime_cols] = df[utc_datetime_cols].apply(pd.to_datetime).apply(lambda x: x.dt.tz_convert(target_time_zone))
    print("Dataframe Head")
    print(df.head())

    csv_data = df.to_csv(index=False)
    print("Data converted to csv string")

    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key+'.txt',
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
    dag_id = 'PL_Fetch_Companies_min_properties_v1',
    description='Fetch Companies from HubSpot API',
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_pull_data_from_api = PythonOperator(
        task_id='Fetch_Companies_and_Properties',
        python_callable=fetch_all_companies
    )
    task_fetch_raw_data_from_s3 = PythonOperator(
        task_id='Fetch_raw_data_from_s3',
        python_callable=fetch_company_json_from_s3
    )
    # create table if not exists...
    task_create_company_table = RedshiftSQLOperator(
        task_id='create_table_if_not_exists',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS public.hubspot_companies (
            id BIGINT,
            name VARCHAR(255),
            hubspot_owner_id BIGINT,
            createdate TIMESTAMP,
            ownername VARCHAR(255)
            );
        """
    )
    task_truncate_company_table = RedshiftSQLOperator(
        task_id='truncate_company_table',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            TRUNCATE TABLE public.hubspot_companies;
        """
    )
    task_transfer_processed_s3_to_redshift = S3ToRedshiftOperator(
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
        'name',
        'hubspot_owner_id',
        'createdate',
        'ownername'
        ]
)



    task_pull_data_from_api >> task_fetch_raw_data_from_s3 >> task_create_company_table >> task_truncate_company_table >> task_transfer_processed_s3_to_redshift
