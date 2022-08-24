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
    schedule_interval='@monthly',
    start_date=datetime(2022, 8, 15),
    max_active_runs=1,
    catchup=False
    ) as ingest_dag:

    copy_script_from_s3 = BashOperator(
        task_id='copy_script_from_s3',
        bash_command=(
            f'{YC_S3} cp s3://{YC_SOURCE_BUCKET}/ml_flow.py '
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
    train_model = BashOperator(
        task_id='train_model',
        bash_command=(
            'spark-submit '
            '/home/ubuntu/ml_flow.py '            
            f'--train_artifact "s3a://{YC_INPUT_DATA_BUCKET}/train.parquet" '
            '--output_artifact "Spark_GBTClassifier_v1" '            
        ),         
    )
    copy_model_to_local = BashOperator(
        task_id='copy_model_to_local',
        bash_command=(
            'hdfs dfs -copyToLocal Spark_GBTClassifier_v1 '
            '/home/ubuntu/Spark_GBTClassifier_v1 '
        )
    )
    copy_model_to_s3 = BashOperator(
       task_id='copy_model_to_s3',
       bash_command=(
        f'{YC_S3} cp /home/ubuntu/Spark_GBTClassifier_v1/ '
        f's3://{YC_OUTPUT_DATA_BUCKET}/Spark_GBTClassifier_v1 --recursive '        
       )
    )