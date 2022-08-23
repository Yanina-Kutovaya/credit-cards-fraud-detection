MODEL = 'feature_extraction_pipeline_model'
YC_S3 = 'aws s3 --endpoint-url=https://storage.yandexcloud.net'
YC_SOURCE_BUCKET = 'airflow-cc-source'      # S3 bucket for pyspark source files
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'   # S3 bucket for input data
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output' # S3 bucket for output data


from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id = 'get_test_features_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 8, 15),
    max_active_runs=1,
    catchup=False
    ) as ingest_dag:

    copy_script_from_s3 = BashOperator(
        task_id='copy_script_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_SOURCE_BUCKET}/generate_test_features.py '
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
    copy_pipeline_model_from_s3 = BashOperator(
        task_id='copy_pipeline_model_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_OUTPUT_DATA_BUCKET}/{MODEL} '
            f'/home/ubuntu/{MODEL} --recursive '
        )   
    )
    copy_raw_data_from_s3 = BashOperator(
        task_id='copy_raw_data_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_INPUT_DATA_BUCKET}/test.parquet '
            '/home/ubuntu/test.parquet '
        )   
    )
    move_pipeline_model_to_hdfs = BashOperator(
        task_id='move_pipeline_model_to_hdfs',
        bash_command = f'hdfs dfs -moveFromLocal /home/ubuntu/{MODEL} '
    )
    move_raw_data_to_hdfs = BashOperator(
        task_id='move_raw_data_to_hdfs',
        bash_command = f'hdfs dfs -moveFromLocal /home/ubuntu/test.parquet '

    )
    generate_test_features = BashOperator(
        task_id='generate_test_features',
        bash_command=(
            'spark-submit '
            '/home/ubuntu/generate_test_features.py '            
        )      
    )
    copy_test_features_to_local = BashOperator(
        task_id='copy_test_features_to_local',
        bash_command=(
            'hdfs dfs -copyToLocal test_features.parquet '
            '/home/ubuntu/test_features.parquet '
        )
    )
    copy_test_features_to_s3 = BashOperator(
       task_id='copy_test_features_to_s3',
       bash_command=(
        f'{YC_S3} cp /home/ubuntu/test_features.parquet/ '
        f's3://{YC_OUTPUT_DATA_BUCKET}/test_features.parquet --recursive '        
       )
    )
    copy_script_from_s3 >> generate_test_features
    copy_custom_transformers_from_s3 >> generate_test_features
    copy_pipeline_model_from_s3 >> move_pipeline_model_to_hdfs >> generate_test_features
    copy_raw_data_from_s3 >> move_raw_data_to_hdfs >> generate_test_features
    generate_test_features >> copy_test_features_to_local >> copy_test_features_to_s3

