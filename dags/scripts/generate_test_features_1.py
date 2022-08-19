APP_NAME = 'Test data preprocessing'

from pyspark.sql import SparkSession
from custom_transformers import (
    DiscreteToBinaryTransformer,
    ContinuousOutliersCapper,
    TimeFeaturesGenerator,
    ScalarNAFiller,
    StringFromDiscrete
)
from pyspark.ml import PipelineModel


def main():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()    
    df_test = spark.read.parquet('test.parquet')
    df_test = df_test.repartition(4)
    feature_extraction_pipeline_model = PipelineModel.load('feature_extraction_pipeline_model')      
    df_test = feature_extraction_pipeline_model.transform(df_test)
    df_test.repartition(1).write.format('parquet').save('test_features.parquet')


if __name__ == "__main__":
    main()