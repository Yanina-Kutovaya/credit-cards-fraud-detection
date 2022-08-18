APP_NAME = 'Feature extraction pipeline model'
YC_INPUT_DATA_BUCKET = 'credit-cards-data'   # S3 bucket for input data
 
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
from feature_extraction_pipeline import get_feature_extraction_pipeline

 
def main():
    conf = SparkConf().setAppName(APP_NAME)
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    sc = SparkContext(conf=conf)
 
    sql = SQLContext(sc)    
    df_train = sql.read.parquet(f's3a://{YC_INPUT_DATA_BUCKET}/train.parquet')
    defaultFS = sc._jsc.hadoopConfiguration().get('fs.defaultFS')
    
    feature_extraction_pipeline = get_feature_extraction_pipeline()
    feature_extraction_pipeline_model = feature_extraction_pipeline.fit(df_train)
    df_train = feature_extraction_pipeline_model.transform(df_train)

    feature_extraction_pipeline_model.save(defaultFS+'/tmp/feature_extraction_pipeline_model')
    df_train.repartition(1).write.parquet(defaultFS+'/tmp/train_features.parquet') 

if __name__ == "__main__":
    main()