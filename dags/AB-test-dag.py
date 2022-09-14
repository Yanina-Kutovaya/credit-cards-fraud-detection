TRAIN_ARTIFACT = 'train.parquet'
SAVED_ARTIFACT = 'Spark_GBTClassifier_v1'
OUTPUT_ARTIFACT = 'Spark_GBTClassifier_v2'

YC_S3 = 'aws s3 --endpoint-url=https://storage.yandexcloud.net'
YC_SOURCE_BUCKET = 'airflow-cc-source'      # S3 bucket for pyspark source files
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'   # S3 bucket for input data
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output' # S3 bucket for output data


from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id = 'AB_test_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 7, 15),  
    max_active_runs=1,
    catchup=False     
    ) as ingest_dag:

    copy_data_from_s3 = BashOperator(
        task_id='copy_data_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_INPUT_DATA_BUCKET}/{TRAIN_ARTIFACT} '
            '/home/ubuntu/train.parquet '
        )   
    )
    move_data_to_hdfs = BashOperator(
        task_id='move_data_to_hdfs',
        bash_command = f'hdfs dfs -moveFromLocal /home/ubuntu/{TRAIN_ARTIFACT} '

    )
    copy_model_from_s3 = BashOperator(
        task_id='copy_model_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_OUTPUT_DATA_BUCKET}/{SAVED_ARTIFACT} '
            f'/home/ubuntu/{SAVED_ARTIFACT} --recursive'
        )   
    )
    move_model_to_hdfs = BashOperator(
        task_id='move_model_to_hdfs',
        bash_command = f'hdfs dfs -moveFromLocal /home/ubuntu/{SAVED_ARTIFACT} '

    )
    copy_script_from_s3 = BashOperator(
        task_id='copy_script_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_SOURCE_BUCKET}/AB_test.py '
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
    copy_fraud_detection_pipeline_from_s3 = BashOperator(
        task_id='copy_fraud_detection_pipeline_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_SOURCE_BUCKET}/fraud_detection_model_pipeline.py '
            f'/home/ubuntu/ '
        )   
    )    
    AB_test = BashOperator(
        task_id='AB_test',
        bash_command=(
            'spark-submit '
            '/home/ubuntu/AB_test.py '            
            f'--train_artifact "{TRAIN_ARTIFACT}" '            
            f'--saved_artifact "{SAVED_ARTIFACT}" '
            f'--output_artifact "{OUTPUT_ARTIFACT}" '            
        ),         
    )
    
    copy_data_from_s3 >> move_data_to_hdfs >> train_model
    copy_model_from_s3 >> move_model_to_hdfs >> train_model
    [
        copy_script_from_s3, 
        copy_custom_transformers_from_s3,
        copy_fraud_detection_pipeline_from_s3
    ] >> AB_test
