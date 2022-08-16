from pyspark.sql import SparkSession

YC_INPUT_DATA_BUCKET = 'airflow-cc-input'
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output'

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

data = spark.read.format('csv')\
    .option('headers', True)\
    .option('delimiter', ',')\
    .load(f's3://{YC_INPUT_DATA_BUCKET}/sensors_data_part_0000*.csv')

## ignore - 'black list' (items to be excluded)
# ignore = spark.sql(sqlQuery='select device_id from ignore')
# data = data.join(ignore, on='device_id', how='left_anti')

data.repartition(1)\
    .write.format('parquet')\
    .save(f's3//{YC_OUTPUT_DATA_BUCKET}/sensors')
