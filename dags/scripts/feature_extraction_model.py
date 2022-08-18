APP_NAME = 'Feature extraction pipeline model'
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'   # S3 bucket for input data
YC_SOURCE_BUCKET = 'airflow-cc-source'      # S3 bucket for pyspark source files

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
    df_train = (
         spark.read.format('csv')
        .option('headers', True)
        .option('delimiter', ',')
        .load('train.csv')
    )
    df_train = df_train.repartition(4)

    feature_extraction_pipeline = get_feature_extraction_pipeline()
    feature_extraction_pipeline_model = feature_extraction_pipeline.fit(df_train)
    feature_extraction_pipeline_model.save('feature_extraction_pipeline_model')

    df_train = feature_extraction_pipeline_model.transform(df_train)
    df_train.repartition(1).write.format('parquet').save('train_features.parquet')


if __name__ == "__main__":
    main()