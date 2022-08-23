APP_NAME = 'Test data preprocessing'

import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from custom_transformers import (
    DiscreteToBinaryTransformer,
    ContinuousOutliersCapper,
    TimeFeaturesGenerator,
    ScalarNAFiller,
    StringFromDiscrete
)
from pyspark.ml import PipelineModel


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def main():    
    logger.info('Creating Spark Session ...')
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    logger.info('Loading feature_extraction_pipeline_model ...')
    feature_extraction_pipeline_model = PipelineModel.load('feature_extraction_pipeline_model')   
       
    logger.info('Loading data ...')    
    data = spark.read.parquet('test.parquet').repartition(4)

    logger.info('Preprocessing data ...')     
    data = feature_extraction_pipeline_model.transform(data)

    logger.info('Saving features ...') 
    data.write.format('parquet').save('test_features.parquet')

    logger.info('Done')
    spark.stop()


if __name__ == "__main__":    
    main()