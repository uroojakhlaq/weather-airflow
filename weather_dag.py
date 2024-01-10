from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago
from weather_etl import final_etl

default_args ={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2024,01,9),
    'email':['airflow@example.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

dag= DAG(
'weather_dag',
default_args=default_args,
description='Weather etl for dehradun',
schedule_interval=timedelta(minutes=60)
)

    
run_etl = PythonOperator(
    task_id='weather_etl_final',
    python_callable=final_etl,
    dag=dag
)
run_etl