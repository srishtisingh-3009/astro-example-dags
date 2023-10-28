import os
from airflow import DAG
from time import time_ns
from datetime import datetime
from airflow.models.connection import Connection
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

def func():
    print("DAG1 is running", )

with DAG(
    dag_id="DAG1", schedule="*/2 * * * *", start_date=datetime(2023, 1, 1), is_paused_upon_creation=False, catchup=False
) as dag:


    task_1 = PythonOperator(
            task_id='Dag1',
            python_callable=func
        )
		
    end_task = DummyOperator(task_id='end_task', dag=dag)

    task_1 >> end_task
