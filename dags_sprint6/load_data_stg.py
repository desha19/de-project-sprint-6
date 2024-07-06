import contextlib
import hashlib
import json
from typing import Dict, List, Optional
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base_hook import BaseHook
 
from airflow.decorators import dag
 
import pandas as pd
import pendulum
import vertica_python

def get_vertica_connection():
    conn_id = 'connect_to_vertica'
    hook = BaseHook.get_hook(conn_id)
    return hook.get_conn()
 
def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len(df)
    vertica_conn = get_vertica_connection()
    #vertica_conn = vertica_python.connect(
        #...  # тут надо прописать ваши параметры соединения. Я создал подключение к vertica в airflow называется "connect_to_vertica"
    #)
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            #cur.copy(copy_expr, df[start:end].to_csv(index=False, header=False))  # тут вам надо прописать команду, которая загружает эту порцию
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1
 
    vertica_conn.close()
 
 
@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_load_data_to_staging():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    load_users = PythonOperator(
        task_id='load_users',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/users.csv',  # путь к скачанному файлу
            'schema': 'STV202406073__STAGING',  # схема, куда загружаем данные
            'table': 'users',  # таблица, в которую будем загружать 
            'columns': ['id', 'сhat_name', 'registration_dt', 'country', 'age'],  # колонки для загрузки
            'type_override': {'age': 'Int64'},  # преобразования типов при загрузке (опционально)
        }
    )
    load_groups = PythonOperator(
        task_id='load_groups',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/groups.csv',  # путь к скачанному файлу
            'schema': 'STV202406073__STAGING',  # схема, куда загружаем данные
            'table': 'groups',  # таблица, в которую будем загружать 
            'columns': ['id', 'admin_id', 'group_name', 'registration_dt', 'is_private']#,  # колонки для загрузки
            #'type_override': {'age': 'Int64'},  # преобразования типов при загрузке (опционально)
        }
    )
    load_dialogs = PythonOperator(
        task_id='load_dialogs',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/dialogs.csv',  # путь к скачанному файлу
            'schema': 'STV202406073__STAGING',  # схема, куда загружаем данные
            'table': 'dialogs',  # таблица, в которую будем загружать 
            'columns': ['message_id', 'message_ts', 'message_from', 'message_to', 'message', 'message_group']#,  # колонки для загрузки
            #'type_override': {'age': 'Int64'},  # преобразования типов при загрузке (опционально)
        }           
    )
 
    start >> [load_users, load_groups, load_dialogs] >> end
 
 
_ = sprint6_dag_load_data_to_staging()