app_name = 'Test data preprocessing' 
import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml import PipelineModel
 
def main():
    conf = SparkConf().setAppName(app_name)
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    sc = SparkContext(conf=conf)
 
    sql = SQLContext(sc)
    df_test = sql.read.parquet('s3a://credit-cards-data/test.parquet')          
    defaultFS = sc._jsc.hadoopConfiguration().get('fs.defaultFS')
    feature_extraction_pipeline_model = PipelineModel.load('feature_extraction_pipeline_model_v1')        
    df_test = feature_extraction_pipeline_model.transform(df_test)         
    df_test.write.parquet(defaultFS+'/tmp/test_features.parquet')    
 
if __name__ == "__main__":
    main()