APP_NAME = 'Test data preprocessing'
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'   # S3 bucket for input data
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output' # S3 bucket for output data

import sys 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from custom_transformers import (
    DiscreteToBinaryTransformer,
    ContinuousOutliersCapper,
    TimeFeaturesGenerator,
    ScalarNAFiller,
    StringFromDiscrete
)
from pyspark.ml import PipelineModel


def main():
    conf = SparkConf().setAppName(APP_NAME)
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    sc = SparkContext(conf=conf)
 
    sql = SQLContext(sc)    
    df_test = sql.read.parquet(f's3a://{YC_INPUT_DATA_BUCKET}/test.parquet')
    df_test = df_test.repartition(4)
    
    feature_extraction_pipeline_model = PipelineModel.load(
        f's3a://{YC_OUTPUT_DATA_BUCKET}/feature_extraction_pipeline_model'
    )
    df_test = feature_extraction_pipeline_model.transform(df_test)
    df_test.repartition(1).write.format('parquet').save('test_features.parquet')


if __name__ == "__main__":
    main()