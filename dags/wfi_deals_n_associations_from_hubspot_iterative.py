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

target_time_zone = 'America/Chicago'
hta_hubspot_token = "pat-na1-047e1c65-3481-4d3e-afc3-57558aa07b8b"
bucket_name = 'wfiredshiftbucket'
s3_aws_conn_id = 'S3_Connection_WFI'
redshift_aws_conn_id = 'Redhsift_DW_WFI'

s3_deals_file_name = 'required_columns_deals'
s3_key_deal_companies = "required_columns_deal_companies_association"
s3_key_deal_contacts = "required_columns_deal_contacts_association"
target_table_name = 'hubspot_deals'

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.__str__()
    
# fetch properties and contacts
def fetch_all_deals():
    client = hubspot.Client.create(access_token=hta_hubspot_token)
    deal_property_dictionary = {    'dealname': 'Deal Name',
                                    'pipeline': 'Pipeline',
                                    'dealtype': 'Deal Type',
                                    'amount': 'Amount',
                                    'dealstage': 'Deal Stage',
                                    'closedate': 'Close Date',
                                    'createdate': 'Create Date',
                                    'hubspot_owner_id': 'Deal owner',
                                    'hubspot_team_id': 'HubSpot Team',
                                    'hs_priority': 'Priority',
                                    'num_associated_contacts': 'Number of Associated Contacts',
                                    'hs_lastmodifieddate': 'Last Modified Date',
                                    'closed_lost_reason': 'Closed Lost Reason',
                                    'closed_won_reason': 'Closed Won Reason',
                                    'amount_in_home_currency': 'Amount in company currency',
                                    'days_to_close': 'Days to close',
                                    'hs_analytics_source': 'Original Source Type', 
                                    'num_contacted_notes': 'Number of times contacted',
                                    'num_notes': 'Number of Sales Activities'
                                    }

    # Iteration and API parameters
    all_deals = []
    after=0
    iteration_no = 1
    limit=100
    
    # Loop until the offset is None...
    while True:
        # Print the Iteration No.
        print(iteration_no)

        # Make a GET request to the /crm/v3/objects/deals endpoint with the query parameters
        api_response = client.crm.deals.basic_api.get_page(limit=100, archived=False, after=after, associations=['COMPANIES', "CONTACTS"], 
                                                           properties=list(deal_property_dictionary.keys()))

        # Extract the deals data from the response
        deals = api_response.results

        # Check if all deals have been retrieved
        if not deals:
            break

        # Add result to all_contact list
        all_deals.extend(deals)

        # Increment the offset for the next request, if api_response is not None
        if api_response.paging and api_response.paging.next and api_response.paging.next.after:
            after = api_response.paging.next.after
        # Break the loop if API response is None
        else:
            break
        # Increment for next iteration
        iteration_no+=1

    print('All Contacts Fetched')
    
    # Convert the HubSpot returned object to a dictionary
    all_deals_dict = [obj.to_dict() for obj in all_deals]
    print("All Objects converted to Dictionary")
    print(f'all_contacts_dict[0]={all_deals_dict[0]}')
    
    # Normalize the json format and fetch companies & contacts associations
    
    # list of company association
    all_companies_associations = []
    # list of contact association
    all_contacts_associations = []
    # loop through the deals dictionary
    for obj in all_deals_dict:
        obj.update(obj.pop('properties'))

        # Check if deal contains an association
        if obj.get('associations'):
            associations = obj.pop('associations')
            
            # if company association exists
            if associations.get('companies'):
                company = associations.pop('companies')
                # list of dictionary with deal_id, company_id, type
                tmp_dict = [{'deal_id': obj['id'], 'company_id':a.get('id'), 'type': a.get('type')} for a in company['results']]
                all_companies_associations.extend(tmp_dict)

            # if contact association exists
            if associations.get('contacts'):
                contact = associations.pop('contacts')
                # list of dictionary with deal_id, contact_id, type
                tmp_dict = [{'deal_id': obj['id'], 'contact_id':a.get('id'), 'type': a.get('type')} for a in contact['results']]
                all_contacts_associations.extend(tmp_dict)


    print('All Contacts Normalized to single attribute pattern')
    print(f'First normalized value: {all_deals_dict[0]}')

    # Convert the response to json to store in s3
    json_data_deals = json.dumps(all_deals_dict, default=datetime_converter)
    json_data_deal_companies_associations = json.dumps(all_companies_associations, default=datetime_converter)
    json_data_deal_contacts_associations = json.dumps(all_contacts_associations, default=datetime_converter)
    print('All Contacts converted to Json')
    # print(f'json_data[0]={json_data[0]}')
    # Build S3 Connection - Connection info in Airflow > Admin > Connections
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)
    print("Connection with s3 build successfully")

    # S3 bucket and file info
    
    delay_sec = 30
    # Save the json file to S3
    retries = 3
    for i in range(retries+1):
        try:
            s3_hook.load_string(json_data_deals, 
                                key=s3_deals_file_name+ ".json", 
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
    # Save the json file for Deal-Company-Association to S3
    retries = 3
    for i in range(retries+1):
        try:
            s3_hook.load_string(json_data_deal_companies_associations, 
                                key=s3_key_deal_companies + ".json", 
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
    # Save the json file for Deal-Contact-Association to S3
    retries = 3
    for i in range(retries+1):
        try:
            s3_hook.load_string(json_data_deal_contacts_associations, 
                                key=s3_key_deal_contacts + ".json", 
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


# Fetch Deals data from s3 and transform to csv
def fetch_deals_json_from_s3(**kwargs):
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)

    # Fetch the JSON file from S3
    json_data = s3_hook.read_key(s3_deals_file_name+ ".json", bucket_name)
    print('Data extracted successfully')

    # Deserialize the JSON data into a Python dictionary
    json_dict = json.loads(json_data)
    print(f'first object of json dict = {json_dict[0]}')

    df = pd.DataFrame.from_dict(json_dict)
    # Data should be in the same sequence as the Redshift target table, as per the Copy statement Docx...

    df = df[[
        'id',
        'dealname',
        'pipeline',
        'dealtype',
        'amount',
        'dealstage',
        'closedate',
        'createdate',
        'hubspot_owner_id',
        'hubspot_team_id',
        'hs_priority',
        'num_associated_contacts',
        'hs_lastmodifieddate',
        'closed_lost_reason',
        'closed_won_reason',
        'amount_in_home_currency',
        'days_to_close',
        'hs_analytics_source', 
        'num_contacted_notes',
        'num_notes'
        ]]
    
     # Convert Timezone for utc columns
    utc_datetime_cols = ['closedate', 'createdate']
    df[utc_datetime_cols] = df[utc_datetime_cols].apply(pd.to_datetime).apply(lambda x: x.dt.tz_convert(target_time_zone))
    # df['createdate'] = pd.to_datetime(arg=df['createdate'])
    # df['createdate'] = df['createdate'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # add a column for Create Date - Month Year formatting...
    print("Dataframe Head")
    print(df.head())

    csv_data = df.to_csv(index=False)
    print("Data converted to csv string")

    s3_hook.load_string(
        string_data=csv_data,
        key=s3_deals_file_name + '.txt',
        bucket_name=bucket_name,
        replace=True
    )
    print("Data saved to csv successfully")

# Fetch Deal-Company data from s3 and transform to csv
def fetch_deal_companies_json_from_s3(**kwargs):
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)

    # Fetch the JSON file from S3
    json_data = s3_hook.read_key(s3_key_deal_companies + ".json", bucket_name)
    print('Data extracted successfully')

    # Deserialize the JSON data into a Python dictionary
    json_dict = json.loads(json_data)
    print(f'first object of json dict = {json_dict[0]}')

    df = pd.DataFrame.from_dict(json_dict)
    # Data should be in the same sequence as the Redshift target table, as per the Copy statement Docx...

    df = df[['deal_id','company_id','type']]
    print("Dataframe Head")
    print(df.head())

    csv_data = df.to_csv(index=False)
    print("Data converted to csv string")

    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key_deal_companies + '.txt',
        bucket_name=bucket_name,
        replace=True
    )
    print("Data saved to csv successfully")

# Fetch Deal-Contacts data from s3 and transform to csv
def fetch_deal_contacts_json_from_s3(**kwargs):
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id=s3_aws_conn_id)

    # Specify the S3 bucket and key for the JSON file

    # Fetch the JSON file from S3
    json_data = s3_hook.read_key(s3_key_deal_contacts  + ".json", bucket_name)

    print('Data extracted successfully')

    # Deserialize the JSON data into a Python dictionary
    json_dict = json.loads(json_data)
    print(f'first object of json dict = {json_dict[0]}')

    df = pd.DataFrame.from_dict(json_dict)

    # Data should be in the same sequence as the Redshift target table, as per the Copy statement Docx...
    df = df[['deal_id','contact_id','type']]
    print("Dataframe Head")
    print(df.head())

    csv_data = df.to_csv(index=False)
    print("Data converted to csv string")

    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key_deal_contacts+'.txt',
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
    dag_id = 'WFI_Fetch_Deals_n_Associations_v1',
    description='Fetch Contacts from HubSpot API',
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_1_pull_deals_from_api = PythonOperator(
        task_id='1_Fetch_Deals_and_Properties',
        python_callable=fetch_all_deals
    )

    task_2a_fetch_deals_raw_data_from_s3 = PythonOperator(
        task_id='2a_Fetch_Deals_raw_data_from_s3',
        python_callable=fetch_deals_json_from_s3
    )
    task_2b_fetch_deal_companies_raw_data_from_s3 = PythonOperator(
        task_id='2b_Fetch_Deal_Companies_raw_data_from_s3',
        python_callable=fetch_deal_companies_json_from_s3
    )
    task_2c_fetch_deal_contacts_raw_data_from_s3 = PythonOperator(
        task_id='2c_Fetch_Deal_Contacts_raw_data_from_s3',
        python_callable=fetch_deal_contacts_json_from_s3
    )
    
    # create deals table if not exists...
    task_3a_create_deals_table = RedshiftSQLOperator(
        task_id='3a_create_deals_table_if_not_exists',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS public.hubspot_deals (
            id BIGINT,
            dealname VARCHAR(255),
            pipeline VARCHAR(255),
            dealtype VARCHAR(255),
            amount NUMERIC(10,2),
            dealstage VARCHAR(255),
            closedate TIMESTAMP,
            createdate TIMESTAMP,
            hubspot_owner_id BIGINT,
            hubspot_team_id BIGINT,
            hs_priority VARCHAR(255) ,
            num_associated_contacts INT,
            hs_lastmodifieddate TIMESTAMP,
            closed_lost_reason VARCHAR(255),
            closed_won_reason VARCHAR(255),
            amount_in_home_currency NUMERIC(10,2),
            days_to_close INT,
            hs_analytics_source VARCHAR(50), 
            num_contacted_notes INT,
            num_notes INT
            );
        """
    )

    task_3b_create_deal_companies_table = RedshiftSQLOperator(
        task_id='3b_create_deal_companies_table_if_not_exists',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS public.hubspot_deal_companies (
            deal_id BIGINT,
            company_id BIGINT,
            type VARCHAR(50)
            );
        """
    )

    task_3c_create_deal_contacts_table = RedshiftSQLOperator(
        task_id='3c_create_deal_contacts_table_if_not_exists',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS public.hubspot_deal_contacts (
            deal_id BIGINT,
            contact_id BIGINT,
            type VARCHAR(50)
            );
        """
    )


    task_4a_truncate_deals_table = RedshiftSQLOperator(
        task_id='4a_truncate_deals_table',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            TRUNCATE TABLE public.hubspot_deals;
        """
    )
    task_4b_truncate_deal_companies_table = RedshiftSQLOperator(
        task_id='4b_truncate_deal_companies_table',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            TRUNCATE TABLE public.hubspot_deal_companies;
        """
    )
    task_4c_truncate_deal_contacts_table = RedshiftSQLOperator(
        task_id='4c_truncate_deal_contacts_table',
        redshift_conn_id=redshift_aws_conn_id,
        sql="""
            TRUNCATE TABLE public.hubspot_deal_contacts;
        """
    )

    transfer_5a_deals_s3_to_redshift = S3ToRedshiftOperator(
        task_id="5a_transfer_deals_s3_csv_to_redshift",
        redshift_conn_id=redshift_aws_conn_id,
        aws_conn_id=s3_aws_conn_id,
        s3_bucket=bucket_name,
        s3_key=s3_deals_file_name + '.txt',
        # method='replace'
        schema="public",
        # delimeter=',',
        table=target_table_name,
        copy_options=["csv ", 
                    "IGNOREHEADER 1",
                    "TIMEFORMAT 'auto'"],
        column_list=[
        'id',
        'dealname',
        'pipeline',
        'dealtype',
        'amount',
        'dealstage',
        'closedate',
        'createdate',
        'hubspot_owner_id',
        'hubspot_team_id',
        'hs_priority',
        'num_associated_contacts',
        'hs_lastmodifieddate',
        'closed_lost_reason',
        'closed_won_reason',
        'amount_in_home_currency',
        'days_to_close',
        'hs_analytics_source', 
        'num_contacted_notes',
        'num_notes'
        ]
    )
    transfer_5b_deal_companies_s3_to_redshift = S3ToRedshiftOperator(
        task_id="5b_transfer_deal_companies_s3_csv_to_redshift",
        redshift_conn_id=redshift_aws_conn_id,
        aws_conn_id=s3_aws_conn_id,
        s3_bucket=bucket_name,
        s3_key=s3_key_deal_companies + '.txt',
        # method='replace'
        schema="public",
        # delimeter=',',
        table='hubspot_deal_companies',
        copy_options=["csv ", 
                    "IGNOREHEADER 1",
                    "TIMEFORMAT 'auto'"],
        column_list=[
        'deal_id',
        'company_id',
        'type'
        ]
    )
    transfer_5c_deal_contacts_s3_to_redshift = S3ToRedshiftOperator(
        task_id="5c_transfer_deal_contacts_s3_csv_to_redshift",
        redshift_conn_id=redshift_aws_conn_id,
        aws_conn_id=s3_aws_conn_id,
        s3_bucket=bucket_name,
        s3_key=s3_key_deal_contacts + '.txt',
        # method='replace'
        schema="public",
        # delimeter=',',
        table='hubspot_deal_contacts',
        copy_options=["csv ", 
                    "IGNOREHEADER 1",
                    "TIMEFORMAT 'auto'"],
        column_list=[
        'deal_id',
        'contact_id',
        'type'
        ]
    )



    task_1_pull_deals_from_api >> task_2a_fetch_deals_raw_data_from_s3 >> task_3a_create_deals_table >> task_4a_truncate_deals_table >> transfer_5a_deals_s3_to_redshift
    task_1_pull_deals_from_api >> task_2b_fetch_deal_companies_raw_data_from_s3 >> task_3b_create_deal_companies_table >> task_4b_truncate_deal_companies_table >> transfer_5b_deal_companies_s3_to_redshift
    task_1_pull_deals_from_api >> task_2c_fetch_deal_contacts_raw_data_from_s3 >> task_3c_create_deal_contacts_table >> task_4c_truncate_deal_contacts_table >> transfer_5c_deal_contacts_s3_to_redshift
    
    
