import requests
import json
import boto3
import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import datetime

def json_scraper(url, file_name, bucket):
    print('start_running')
    response = requests.request("GET", url)
    json_data=response.json()
    
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)
        
    s3 = boto3.client('s3')
    s3.upload_file(file_name, bucket, f"predictit/{file_name}")
        
    print('end running')

start_date = datetime.datetime(2024, 3, 19)

default_args = {
    "owner": "clu0501",
    "depends_on_past": False,
    "email": ["clu0501@hotmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    "raw_predictit",
    default_args=default_args,
    description="Calls PredictIt API, writes data as JSON file to Amazon S3.",
    schedule_interval=datetime.timedelta(days=1),
    start_date=start_date
) as dag:
    
    extract_predictit = PythonOperator(
        task_id='extract_predictit',
        python_callable=json_scraper,
        op_kwargs={
            'url':"https://www.predictit.org/api/marketdata/all/",
            'file_name':'predictit_markets.json',
            'bucket':'clu0501-predictit-raw'
        },
        dag=dag
    )
    
    ready = EmptyOperator(task_id='ready')
    extract_predictit >> ready
    