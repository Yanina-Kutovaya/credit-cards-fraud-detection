import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
 
def main():
    conf = SparkConf().setAppName("Month Stat - Python")
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    sc = SparkContext(conf=conf)
 
    sql = SQLContext(sc)
    df = sql.read.parquet("s3a://yc-mdb-examples/dataproc/example01/set01")
    defaultFS = sc._jsc.hadoopConfiguration().get("fs.defaultFS")
    month_stat = df.groupBy("Month").count()
    month_stat.repartition(1).write.format("csv").save(defaultFS+"/tmp/month_stat")
 
if __name__ == "__main__":
    main()