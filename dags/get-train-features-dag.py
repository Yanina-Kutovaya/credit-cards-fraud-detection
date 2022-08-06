from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from datetime import datetime, timedelta

 
default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5)
    }
dag = DAG(
    dag_id = 'get_train_features_dag',    
    default_args=default_args,
    start_date=datetime(2022, 8, 1),    
    schedule_interval='@once',
    description='Generate feature_extraction_pipeline and train_features',
    template_searchpath=' /home/ubuntu/airflow/dags/scripts/'    
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
print('Copy train_features from hdfs to local')
copy_train_features_to_local = BashOperator(
    task_id='copy_train_features_to_local',
    bash_command='hadoop fs -copyToLocal train_features.parquet ',
    dag=dag    
    )
print('Save feature_extraction_pipeline_model_v1 to object storage')
save_model_to_object_storage = BashOperator(
    task_id='save_model_to_object_storage',
    bash_command='aws --endpoint-url=https://storage.yandexcloud.net s3 cp --recursive \
        feature_extraction_pipeline_model_v1 s3://credit-cards-data/feature_extraction_pipeline_model_v1/ ',
    dag=dag    
    )
print('Save train_features to object storage')
save_train_features_to_object_storage = BashOperator(
    task_id='save_train_features_to_object_storage',
    bash_command='aws --endpoint-url=https://storage.yandexcloud.net s3 cp --recursive \
        train_features.parquet s3://credit-cards-data/train_features.parquet ',
    dag=dag    
    )
generate_model_and_train_features >> copy_model_to_local >> save_model_to_object_storage
generate_model_and_train_features >> copy_train_features_to_local >> save_train_features_to_object_storage