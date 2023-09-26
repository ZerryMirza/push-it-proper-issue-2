from airflow import DAG
from airflow.operators.python import PythonOperator
from hubspot.crm.contacts import ApiException
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
import hubspot
import json


def datetime_converter(o):
    if isinstance(o, datetime):
        return o.__str__()
    
# fetch properties and contacts
def fetch_all_contacts():
    client = hubspot.Client.create(access_token="pat-na1-d46a3420-5380-48c2-aa90-60501a87d793")
    try:
        contact_api_response = client.crm.properties.core_api.get_all(object_type="CONTACTS")
        # pprint(contact_api_response)
    except ApiException as e:
        print("Exception when calling basic_api->get_by_id: %s\n" % e)
        pass
    contact_property_dictionary = dict()
    for i in contact_api_response.results:
        contact_property_dictionary.update({i.name:i.label})
    print(contact_property_dictionary)
    # fetch all contacts
    all_contacts = client.crm.contacts.get_all()
    # properties=list(contact_property_dictionary.keys())
    # single_api_response = client.crm.contacts.basic_api.get_by_id(contact_id="3078301", archived=False, properties=list(contact_property_dictionary.keys()))
    # single_api_response_dict = single_api_response.to_dict()
    print('All Contacts Fetched')
    all_contacts_dict = [obj.to_dict() for obj in all_contacts]
    print("objects converted to dict")
    # convert the hubSpot returned object to a dictionary
    # convert the response to json to store in s3
    json_data = json.dumps(all_contacts_dict, default=datetime_converter)
    print('All Contacts converted to Json')
    s3_hook = S3Hook(aws_conn_id="S3_Connection_ED_Tech")
    bucket_name = "zubairtests3ci"
    s3_key = "single_test_contacts_sample.json"
    s3_hook.load_string(json_data, key=s3_key, bucket_name=bucket_name, replace=True)
    print("File stored in S3")

default_args = {
    'owner': 'dell',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id = 'Fetch_Contacts_v2',
    description='Fetch Contacts from HubSpot API',
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='Fetch_Contacts_and_Properties',
        python_callable=fetch_all_contacts
    )

    task1
