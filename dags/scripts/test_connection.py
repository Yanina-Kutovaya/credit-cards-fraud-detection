from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.parquet('s3a://airflow-cc-input/test.parquet')

    a = data.count()
    print(f'data.count() = {a}')

if __name__ == '__main__':
    main()

