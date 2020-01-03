import socket
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return f"Hello world! from_host {socket.gethostname()} {os.getenv('HOSTNAME')}"

dag = DAG('hello_world', description='Simple HellowWorld Test DAG',
          schedule_interval='*/5 * * * *',
          start_date=datetime(2020, 1, 2), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

dummy_operator >> hello_operator
