MODEL = 'feature_extraction_pipeline_model'
YC_S3 = 'aws s3 --endpoint-url=https://storage.yandexcloud.net'
YC_SOURCE_BUCKET = 'airflow-cc-source'      # S3 bucket for pyspark source files
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output' # S3 bucket for output data

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

 
dag = DAG(
    dag_id = 'get_train_features_dag',
    start_date=datetime(2022, 8, 15),
    schedule_interval='@once',
    description='Generate feature_extraction_pipeline_model and train_features'    
)
copy_custom_transformers_from_s3 = BashOperator(
    task_id='copy_custom_transformers_from_s3',
    bash_command=f'{YC_S3} cp s3://{YC_SOURCE_BUCKET}/custom_transformers.py /home/ubuntu/ ',
    dag=dag    
)
copy_pipeline_from_s3 = BashOperator(
    task_id='copy_pipeline_from_s3',
    bash_command=f'{YC_S3} cp s3://{YC_SOURCE_BUCKET}/feature_extraction_pipeline.py /home/ubuntu/ ',
    dag=dag    
)
copy_script_from_s3 = BashOperator(
    task_id='copy_script_from_s3',
    bash_command=f'{YC_S3} cp s3://{YC_SOURCE_BUCKET}/feature_extraction_model.py /home/ubuntu/ ',
    dag=dag    
)
generate_model_and_train_features = SparkSubmitOperator(
    task_id='generate_model_and_train_features',
    application = '/home/ubuntu/feature_extraction_model.py',
    dag=dag
)
save_model_to_s3 = BashOperator(
    task_id='save_model_to_s3',
    bash_command = f'{YC_S3} cp --recursive {MODEL} \
        s3://{YC_OUTPUT_DATA_BUCKET}/{MODEL} ',
    dag=dag
)
save_train_features_to_s3 = BashOperator(
    task_id='save_train_features_to_s3',
    bash_command = f'{YC_S3} cp --recursive train_features.parquet \
        s3://{YC_OUTPUT_DATA_BUCKET}/train_features.parquet ',
    dag=dag
)
[copy_custom_transformers_from_s3, copy_pipeline_from_s3, copy_script_from_s3] >> generate_model_and_train_features 
generate_model_and_train_features >> [save_model_to_s3, save_train_features_to_s3]