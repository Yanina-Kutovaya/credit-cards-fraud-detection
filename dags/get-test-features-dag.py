from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from datetime import datetime, timedelta

 
dag = DAG(
    dag_id = 'get_test_features_dag',
    start_date=datetime(2022, 8, 1),
    schedule_interval='@daily',
    template_searchpath='/home/ubuntu/airflow/dags/scripts/'
    )
print('Generate test_features and save them in hdfs')
generate_test_features = SparkSubmitOperator(
    task_id='generate_test_features',
    application = 'generate_test_features.py',
    conn_id = 'spark_local', 
    dag=dag
    )
print('Copy test_features from hdfs to local')
copy_test_features_to_local = BashOperator(
    task_id='copy_test_features_to_local',
    bash_command='hadoop fs -copyToLocal test_features.parquet ',
    dag=dag    
    )
print('Save test_features to object storage')
save_test_features_to_object_storage = BashOperator(
    task_id='save_test_features_to_object_storage',
    bash_command = 'aws --endpoint-url=https://storage.yandexcloud.net s3 cp --recursive \
        test_features.parquet s3://credit-cards-data/test_features.parquet ',
    dag=dag
    )
generate_test_features >> copy_test_features_to_local >> save_test_features_to_object_storage

