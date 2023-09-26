from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import json
import requests
import pandas as pd
import time


# Task 0 to extract the Cam Token from API and store in the Token Handler Class/push to xcom
# Task 1 to extract the data and save it in S3 in Json Format...
# Task 2 to load Json from S3, fetch the Processed Image attributes & drop filemeta & then save the json to S3...
# Task 3 to normalize the json, convert to pandas df and save back to S3.
# Repeat for all Cam Devices
# Either all Cam Devices need to be merged to one csv or read using manifest for multiple csv files...


bucket_name = "camanalysisdata"
s3_key = "cam_raw_json_257"

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.__str__()


def fetch_date_from_api():
    url_storage_events = "https://web.skyvr.video.bluemonitor.net/api/v2/storage/events/"
    # Current Authorization for Camera-257
    storage_event_header = {
        "accept": "application/json",
        "Authorization": "Acc eyJjYW1pZCI6IDI1NywgImNtbmdyaWQiOiAyNTgsICJhY2Nlc3MiOiAiYWxsIiwgInRva2VuIjogInNoYXJlLmV5SnphU0k2SURZd00zMC42M2UwMzAyYnRiYzFmYWIwMC5jTFFQNlp4Z1J3SVZ5VHlDcVRZMDVVU3ByQ3MiLCAiYXBpIjogIndlYi5za3l2ci52aWRlby5ibHVlbW9uaXRvci5uZXQiLCAiY2FtIjogImNhbS5za3l2ci52aWRlby5ibHVlbW9uaXRvci5uZXQifQ=="
    }
    params = {
        "include_filemeta_download": "true",
        "include_meta": "true",
        "limit": 100,
        "offset": 0
    }
    iteration = 1
    all_objects = []

    while True:
        print(f'iteration# {iteration}')
        print(f"offset: {params['offset']}")
        response = requests.get(url=url_storage_events, headers=storage_event_header, params=params)
        data = response.json()
        all_objects.extend(data['objects'])
        
        if data['meta']['next'] is None:
            break
            
        next_url = data['meta']['next']
        next_offset = next_url.split('offset')[1].split('=')[1]
        params['offset'] = next_offset

    # print(len(all_objects))
    all_objects_json = json.dumps(all_objects,  default=datetime_converter)
    s3_hook = S3Hook(aws_conn_id="S3_Connection_BM")
    
    s3_hook.load_string(all_objects_json, key=s3_key + '.json', bucket_name=bucket_name, replace=True)
    print("File stored in S3")


def fetch_deal_companies_json_from_s3(**kwargs):
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id='S3_Connection_ED_Tech')

    # Fetch the JSON file from S3
    json_data = s3_hook.read_key(s3_key + ".json", bucket_name)
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
        key=s3_key + '.txt',
        bucket_name=bucket_name,
        replace=True
    )
    print("Data saved to csv successfully")


default_args = {
    'owner': 'dell',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="cam_test_json_to_s3_v1",
    description="cam api json to s3 csv",
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule='@daily',
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='fetch_raw_data_task',
        python_callable=fetch_date_from_api
    )
    task1