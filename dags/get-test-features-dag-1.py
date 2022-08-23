MODEL = 'feature_extraction_pipeline_model'
YC_S3 = 'aws s3 --endpoint-url=https://storage.yandexcloud.net'
YC_SOURCE_BUCKET = 'airflow-cc-source'      # S3 bucket for pyspark source files
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'   # S3 bucket for input data
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output' # S3 bucket for output data


from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id = 'get_test_features_dag_1',
    schedule_interval='@daily',
    start_date=datetime(2022, 8, 15),
    max_active_runs=1,
    catchup=False
    ) as ingest_dag:

    copy_script_from_s3 = BashOperator(
        task_id='copy_script_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_SOURCE_BUCKET}/generate_test_features_1.py '
            '/home/ubuntu/ '
        )
    )
    copy_custom_transformers_from_s3 = BashOperator(
        task_id='copy_custom_transformers_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_SOURCE_BUCKET}/custom_transformers.py '
            '/home/ubuntu/ ' 
        )  
    )      
    generate_test_features = BashOperator(
        task_id='generate_test_features',
        bash_command=(
            'spark-submit '
            '/home/ubuntu/generate_test_features.py '
            f'--model_artifact "s3a//{YC_OUTPUT_DATA_BUCKET}/{MODEL}" '
            f'--raw_data_artifact "s3a//{YC_INPUT_DATA_BUCKET}/test.parquet" '
            f'--output_artifact "s3a//{YC_OUTPUT_DATA_BUCKET}/test_features.parquet" '            
        ),         
    )    
    [copy_script_from_s3, copy_custom_transformers_from_s3] >> generate_test_features
    
    


