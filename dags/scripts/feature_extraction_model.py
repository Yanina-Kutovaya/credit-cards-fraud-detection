APP_NAME = 'Feature extraction pipeline model'
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'   # S3 bucket for input data
YC_SOURCE_BUCKET = 'airflow-cc-source'      # S3 bucket for pyspark source files
TRAIN_DATA_URL = 'https://storage.yandexcloud.net/credit-cards-data/train.parquet'

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from custom_transformers import *
from feature_extraction_pipeline import get_feature_extraction_pipeline

def main():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.addFile(TRAIN_DATA_URL)
    df_train = spark.read.parquet(
        SparkFiles.get('train.parquet'), header=True, inferSchema=True
    )
    df_train = df_train.repartition(4)

    feature_extraction_pipeline = get_feature_extraction_pipeline()
    feature_extraction_pipeline_model = feature_extraction_pipeline.fit(df_train)
    feature_extraction_pipeline_model.save('feature_extraction_pipeline_model')

    df_train = feature_extraction_pipeline_model.transform(df_train)
    df_train.repartition(1).write.format('parquet').save('train_features.parquet')


if __name__ == "__main__":
    main()