MODEL = 'feature_extraction_pipeline_model'
YC_S3 = 'aws s3 --endpoint-url=https://storage.yandexcloud.net'
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'   # S3 bucket for input data
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output' # S3 bucket for output data
YC_SOURCE_BUCKET = 'airflow-cc-source'      # S3 bucket for pyspark source files

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

 
dag = DAG(
    dag_id = 'get_test_features_dag',
    start_date=datetime(2022, 8, 15),
    schedule_interval='@daily',
    description='Generate test_features from feature_extraction_pipeline_model and test.parquet'    
)
get_model_from_s3 = BashOperator(
    task_id='get_model_from_s3',
    bash_command = f'{YC_S3} cp s3://{YC_OUTPUT_DATA_BUCKET}/{MODEL} . ',
    dag=dag 
)
generate_test_features = SparkSubmitOperator(
    task_id='generate_test_features',
    application = '/home/ubuntu/airflow/dags/scripts/generate_test_features_1.py',
    dag=dag
)
copy_test_features_to_local = BashOperator(
    task_id='copy_test_features_to_local',
    bash_command='hdfs dfs -copyToLocal test_features.parquet ',
    dag=dag    
)
save_test_features_to_s3 = BashOperator(
    task_id='save_test_features_to_s3',
    bash_command = f'{YC_S3} cp --recursive /home/ubuntu/test_features.parquet \
        s3://{YC_OUTPUT_DATA_BUCKET}/test_features.parquet ',
    dag=dag
)
generate_test_features >> copy_test_features_to_local >> save_test_features_to_s3