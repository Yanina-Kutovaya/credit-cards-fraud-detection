APP_NAME = 'Test data preprocessing'
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'   # S3 bucket for input data

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from custom_transformers import (
    DiscreteToBinaryTransformer,
    ContinuousOutliersCapper,
    TimeFeaturesGenerator,
    ScalarNAFiller,
    StringFromDiscrete
)
from pyspark.ml import PipelineModel


def main(): 
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .enableHiveSupport()
        .getOrCreate()    
    )
    TEST_DATA_URL = f'https://storage.yandexcloud.net/{YC_INPUT_DATA_BUCKET}/test.csv'

    spark.sparkContext.addFile(TEST_DATA_URL)
    df_test = spark.read.parquet(
        SparkFiles.get('test.parquet'), header=True, inferSchema= True
    )    
    df_test = df_test.repartition(4)
    feature_extraction_pipeline_model = PipelineModel.load('feature_extraction_pipeline_model')
    df_test = feature_extraction_pipeline_model.transform(df_test)
    df_test.repartition(1).write.format('parquet').save('test_features.parquet')


if __name__ == "__main__":
    main()