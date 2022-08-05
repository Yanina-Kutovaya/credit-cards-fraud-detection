app_name = 'Train data preprocessing' 
import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from scripts.feature_extraction_pipeline_v1 import get_feature_extraction_pipeline
 
def main():
    conf = SparkConf().setAppName(app_name)
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    sc = SparkContext(conf=conf)
 
    sql = SQLContext(sc)
    df_train = sql.read.parquet('s3a://credit-cards-data/train.parquet')    
    defaultFS = sc._jsc.hadoopConfiguration().get('fs.defaultFS')


    df_train = df_train.repartition(8)
    feature_extraction_pipeline = get_feature_extraction_pipeline()
    feature_extraction_pipeline_model = feature_extraction_pipeline.fit(df_train)
    df_train = feature_extraction_pipeline_model.transform(df_train)

    feature_extraction_pipeline_model.save(defaultFS+'/tmp/feature_extraction_pipeline_model_v1')
    df_train.write.parquet(defaultFS+'/tmp/train_features.parquet')    
 
if __name__ == "__main__":
    main()