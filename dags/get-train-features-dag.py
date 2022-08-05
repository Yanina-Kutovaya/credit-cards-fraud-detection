from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from datetime import datetime, timedelta

 
default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    #'end_date': datetime(),
    #'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }
dag = DAG(
    dag_id = 'get_train_features_dag',    
    default_args=default_args,
    start_date=datetime(2022, 8, 1),
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',
    description='Generate feature_extraction_pipeline and train_features',
    template_searchpath='/opt/scripts'
    )
print('Generate feature_extraction_pipeline_model and train_features and save them in hdfs')
generate_model_and_train_features = SparkSubmitOperator(
    task_id='generate_model_and_train_features',
    application = 'generate_train_features.py',
    conn_id = 'spark_local', 
    dag=dag
    )
print('Copy feature_extraction_pipeline_model_v1 from hdfs to local')
copy_model_to_local = BashOperator(
    task_id='copy_model_to_local',
    bash_command='hadoop fs -copyToLocal feature_extraction_pipeline_model_v1 ',
    dag=dag    
    )
print('Copy  train_features from hdfs to local')
copy_train_features_to_local = BashOperator(
    task_id='copy_train_features_to_local',
    bash_command='hadoop fs -copyToLocal train_features.parquet ',
    dag=dag    
    )
print('Copy feature_extraction_pipeline_model_v1 to object storage')
copy_model_to_object_storage = BashOperator(
    task_id='copy_model_to_object_storage',
    bash_command='aws --endpoint-url=https://storage.yandexcloud.net s3 cp --recursive \
        feature_extraction_pipeline_model_v1 s3://credit-cards-data/feature_extraction_pipeline_model_v1/',
    dag=dag    
    )
print('Copy train_features to object storage')
copy_train_features_to_object_storage = BashOperator(
    task_id='copy_train_features_to_object_storage',
    bash_command='aws --endpoint-url=https://storage.yandexcloud.net s3 cp --recursive \
        train_features.parquet s3://credit-cards-data/train_features.parquet',
    dag=dag    
    )
generate_model_and_train_features >> copy_model_to_local >> copy_model_to_object_storage
generate_model_and_train_features >> copy_train_features_to_local >> copy_train_features_to_object_storage