"""
S3 Sensor Connection Test
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 8, 6),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_dag_test', default_args=default_args, schedule_interval='@once')

t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "hello, it should work" > s3_conn_test.txt',
    dag=dag)

sensor = S3KeySensor(
    task_id='check_s3_for_file_in_s3',
    bucket_key='test.parquet',
    wildcard_match=True,
    bucket_name='credit-cards-data',
    s3_conn_id='my_conn_S3',
    timeout=18*60*60,
    poke_interval=120,
    dag=dag)

t1.set_upstream(sensor)