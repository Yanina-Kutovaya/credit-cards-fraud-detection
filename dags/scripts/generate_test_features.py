APP_NAME = 'Test data preprocessing'
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'   # S3 bucket for input data
YC_SOURCE_BUCKET = 'airflow-cc-source'      # S3 bucket for pyspark source files

from pyspark.sql import SparkSession
from custom_transformers import *
from pyspark.ml import PipelineModel


def main():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .enableHiveSupport()
        .getOrCreate()
    )
    df_test = (
        spark.read.format('csv')
        .option('headers', True)
        .option('delimiter', ',')
        .load('test.csv')
    )
    df_test = df_test.repartition(4)
    feature_extraction_pipeline_model = PipelineModel.load('feature_extraction_pipeline_model')
    df_test = feature_extraction_pipeline_model.transform(df_test)
    df_test.repartition(1).write.format('parquet').save('test_features.parquet')


if __name__ == "__main__":
    main()