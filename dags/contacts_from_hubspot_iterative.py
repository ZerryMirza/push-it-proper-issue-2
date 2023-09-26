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

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.__str__()
    
# fetch properties and contacts
def fetch_all_contacts():
    client = hubspot.Client.create(access_token="pat-na1-d46a3420-5380-48c2-aa90-60501a87d793")
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
    contact_property_dictionary = {'firstname': 'First Name',
                                   'lastname': 'Last Name',
                                   'email': 'Email',
                                   # 'owneremail': 'HubSpot Owner Email (legacy)',
                                   'mobilephone': 'Mobile Phone Number',
                                   'phone': 'Phone Number',
                                   'hs_user_ids_of_all_owners': 'User IDs of all owners',
                                   'hubspot_owner_id': 'Contact owner',
                                   # 'ownername': 'HubSpot Owner Name (legacy)',
                                   'company': 'Company Name',
                                   'associatedcompanyid': 'Primary Associated Company ID',
                                   'notes_last_updated': 'Last Activity Date',
                                   'notes_next_activity_date': 'Next Activity Date',
                                   'hs_lead_status': 'Lead Status',
                                   'hs_marketable_status': 'Marketing contact status',
                                   'createdate': 'Create Date',
                                   'cognism': 'Cognism',
                                   'hs_analytics_source_data_2': 'Original Source Drill-Down 2'}

    # Iteration and API parameters
    all_contacts = []
    after=0
    iteration_no = 1
    limit=100
    
    # Loop until the offset is None...
    while True:
        # Print the Iteration No.
        print(iteration_no)

        # Make a GET request to the /crm/v3/objects/contacts endpoint with the query parameters
        api_response = client.crm.contacts.basic_api.get_page(limit=limit, archived=False, after=after, properties=list(contact_property_dictionary.keys()))

        # Extract the contacts data from the response
        contacts = api_response.results

        # Check if all contacts have been retrieved
        if not contacts:
            break

        # Add result to all_contact list
        all_contacts.extend(contacts)

        # Increment the offset for the next request, if api_response is not None
        if api_response.paging and api_response.paging.next and api_response.paging.next.after:
            after = api_response.paging.next.after
        # Break the loop if API response is None
        else:
            break
        # Increment for next iteration
        iteration_no+=1

    print('All Contacts Fetched')
    
    # convert the hubSpot returned object to a dictionary
    all_contacts_dict = [obj.to_dict() for obj in all_contacts]
    print("All Objects converted to Dictionary")
    print(f'all_contacts_dict[0]={all_contacts_dict[0]}')
    # normalize the json format
    for obj in all_contacts_dict:
        obj.update(obj.pop('properties'))
    print('All Contacts Normalized to single attribute pattern')
    print(f'First normalized value: {all_contacts_dict[0]}')

    # Convert the response to json to store in s3
    json_data = json.dumps(all_contacts_dict, default=datetime_converter)
    print('All Contacts converted to Json')
    # print(f'json_data[0]={json_data[0]}')
    # Build S3 Connection - Connection info in Airflow > Admin > Connections
    s3_hook = S3Hook(aws_conn_id="S3_Connection_ED_Tech")
    print("Connection with s3 build successfully")

    # S3 bucket and file info
    bucket_name = "zubairtests3ci"
    s3_key = "required_columns_contacts.json"
    retries = 3
    delay_sec = 30
    # Save the json file to S3
    for i in range(retries+1):
        try:
            s3_hook.load_string(json_data, 
                                key=s3_key, 
                                bucket_name=bucket_name, 
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
    s3_hook = S3Hook(aws_conn_id='S3_Connection_ED_Tech')

    # Specify the S3 bucket and key for the JSON file
    s3_bucket_name = 'zubairtests3ci'
    s3_key = 'required_columns_contacts.json'

    # Fetch the JSON file from S3
    json_data = s3_hook.read_key(s3_key, s3_bucket_name)

    print('Data extracted successfully')

    # Deserialize the JSON data into a Python dictionary
    json_dict = json.loads(json_data)
    print(f'first object of json dict = {json_dict[0]}')

    df = pd.DataFrame.from_dict(json_dict)
    # Data should be in the same sequence as the Redshift target table, as per the Copy statement Docx...
    df = df[[
        'id',
        'firstname',
        'lastname',
        'email',
        'associatedcompanyid',
        'company',
        'mobilephone',
        'phone',
        'cognism',
        'hubspot_owner_id',
        'archived',
        'archived_at',
        'hs_analytics_source_data_2',
        'hs_lead_status',
        'hs_marketable_status',
        'hs_user_ids_of_all_owners',
        'created_at',
        'updated_at',
        'createdate',
        'lastmodifieddate',
        'notes_last_updated',
        'notes_next_activity_date'
        ]]
    # Convert Timezone for utc columns
    utc_datetime_cols = ['created_at', 'updated_at', 'createdate', 'lastmodifieddate', 'notes_last_updated', 'notes_next_activity_date']
    df[utc_datetime_cols] = df[utc_datetime_cols].apply(pd.to_datetime).apply(lambda x: x.dt.tz_convert(target_time_zone))
    print("Dataframe Head")
    print(df.head())

    csv_data = df.to_csv(index=False)
    print("Data converted to csv string")

    s3_hook.load_string(
        string_data=csv_data,
        key='required_columns_contacts.txt',
        bucket_name='zubairtests3ci',
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

s3_file_name = 'required_columns_contacts.txt'
bucket_name = 'zubairtests3ci'
s3_aws_conn_id = 'S3_Connection_ED_Tech'
target_table_name = 'hubspot_contacts'


with DAG(
    dag_id = 'Fetch_Contact_min_properties_final',
    description='Fetch Contacts from HubSpot API',
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_pull_contacts_from_api = PythonOperator(
        task_id='Fetch_Contacts_and_Properties',
        python_callable=fetch_all_contacts
    )
    task_fetch_raw_data_from_s3 = PythonOperator(
        task_id='Fetch_raw_data_from_s3',
        python_callable=fetch_json_from_s3
    )
    # create table if not exists...
    task_create_contacts_table = RedshiftSQLOperator(
        task_id='create_table_if_not_exists',
        redshift_conn_id='Redhsift_DW_EdTech',
        sql="""
            CREATE TABLE IF NOT EXISTS public.hubspot_contacts (
            id BIGINT,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            associatedcompanyid BIGINT,
            company VARCHAR(255),
            mobilephone VARCHAR(50),
            phone VARCHAR(50),
            cognism VARCHAR(255),
            hubspot_owner_id BIGINT,
            archived BOOLEAN,
            archived_at TIMESTAMP,
            hs_analytics_source_data_2 VARCHAR(255),
            hs_lead_status VARCHAR(255),
            hs_marketable_status BOOLEAN,
            hs_user_ids_of_all_owners BIGINT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            createdate TIMESTAMP,
            lastmodifieddate TIMESTAMP,
            notes_last_updated TIMESTAMP,
            notes_next_activity_date TIMESTAMP
            );
        """
    )
    task_truncate_contacts_table = RedshiftSQLOperator(
        task_id='truncate_company_table',
        redshift_conn_id='Redhsift_DW_EdTech',
        sql="""
            TRUNCATE TABLE public.hubspot_contacts;
        """
    )
    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_s3_csv_to_redshift",
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
                    "IGNOREHEADER 1",
                    "TIMEFORMAT 'auto'"],
        column_list=[
        'id',
        'firstname',
        'lastname',
        'email',
        'associatedcompanyid',
        'company',
        'mobilephone',
        'phone',
        'cognism',
        'hubspot_owner_id',
        'archived',
        'archived_at',
        'hs_analytics_source_data_2',
        'hs_lead_status',
        'hs_marketable_status',
        'hs_user_ids_of_all_owners',
        'created_at',
        'updated_at',
        'createdate',
        'lastmodifieddate',
        'notes_last_updated',
        'notes_next_activity_date'
        ]
)



    task_pull_contacts_from_api >> task_fetch_raw_data_from_s3 >> task_create_contacts_table >> task_truncate_contacts_table >> transfer_s3_to_redshift
