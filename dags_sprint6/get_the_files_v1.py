from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

import boto3

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def fetch_s3_file(bucket, key, filename):
    s3_client.download_file(
    Bucket=bucket,
    Key=key,
    Filename=filename
)

def print_first_10_lines(filename):
    bash_command = f"head -n 10 /data/{filename}"
    return bash_command

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 7, 13),
    'retries': 1
}

bucket_files = ['users.csv', 'groups.csv', 'dialogs.csv']

with DAG('download_files_and_check', schedule_interval=None, default_args=default_args) as dag:
    
    tasks = []
    for file in bucket_files:
        fetch_task = PythonOperator(
            task_id=f'download_{file}',
            python_callable=fetch_s3_file,
            op_args=('sprint6', file, f'/data/{file}')
        )
        tasks.append(fetch_task)

    check_task = BashOperator(
        task_id='check_files_download',
        bash_command='\n'.join([print_first_10_lines(file) for file in bucket_files])
    )

    for task in tasks:
        task >> check_task