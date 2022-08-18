import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectOperator
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator
)

YC_DP_FOLDER_ID = 'b1gnvl119oudpdqs9nr2' # YC catalog to create cluster
YC_DP_CLUSTER_NAME = f'tmp-dp-{uuid.uuid4()}' # Name for temporary DP cluster
YC_DP_CLUSTER_DESC = 'Temporary cluster for Spark processing under Airflow orcesration' # DP cluster description
YC_DP_SUBNET_ID = 'e9bpckbmt2sim9jnupht' # YC subnet to create cluster
YC_DP_SA_ID = 'aje2hg524orhj5bsufro' # YC sevice account for Data Proc cluster

YC_INPUT_DATA_BUCKET = 'airflow-cc-input' # S3 bucket for input data
YC_SOURCE_BUCKET = 'airflow-cc-source' # S3 bucket for pyspark source files
YC_DP_LOGS_BUCKET = 'airflow-cc-logs' # S3 bucket for Data Proc cluster logs

with DAG(
    'DATA_INGEST',
    schedule_interval='@hourly',
    tags = ['airflow-cc'],
    start_date=datetime.datetime.now(),
    max_active_runs=1,
    catchup=False
) as ingest_dag:

    s3_sensor = S3KeySensor(
        task_id='s3-sensor-task',
        backet_key = 'sensors-data-*.csv',
        bucket_name=YC_INPUT_DATA_BUCKET,
        wildcard_match=True,
        aws_conn_id='yc-s3',
        poke_interval=10,
        dag=ingest_dag        
    )

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        folder_id=YC_DP_FOLDER_ID,
        cluster_name=YC_DP_CLUSTER_NAME,
        cluster_description=YC_DP_CLUSTER_DESC,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        zone='ru-central1-a',
        service_account_id=YC_DP_SA_ID,
        cluster_image_version='2.0.43',
        enable_ui_proxy=False,
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-ssd',
        computenode_disk_size=200,
        computenode_count=2,
        computenode_max_hosts_count=5,
        services=['YARN', 'SPARK'], # Creating lightweight Spark cluster
        datanode_count=0, # With no data nodes
        properties={# But pointing it to remote Metastore cluster
        # Pre-created persistent light cluster with HDFS, HIVE, MAPREDUCE, YARN
        # from MASTERNODE and DATANODE
        'spark:spark.hive.matastore.uris': 'thrift://rc1a-dataproc-m-zqbxjx006jc7qwiq.mdb.yandexcloud.net:9083',
        'spark:spark.hive.matastore.warehose.dir:': 's3a://dataproc-1/metastore/'
        },
        connection_id='yc-airflow-sa',
        dag=ingest_dag
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/data_processing.py',
        connection_id='yc-airflow-sa',
        dag=ingest_dag
    )

    clean_input_bucket = S3DeleteObjectOperator(
        task_id='clean_input_bucket',
        prefix='sensors-data-part-0000',
        backet=YC_INPUT_DATA_BUCKET,
        aws_conn_id='yc-s3',
        dag=ingest_dag
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=ingest_dag
    )

    s3_sensor >> create_spark_cluster >> poke_spark_processing >> clean_input_bucket >> delete_spark_cluster