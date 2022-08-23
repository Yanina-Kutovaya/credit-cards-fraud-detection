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

def main(args):    
    logger.info('Creating Spark Session ...')
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    logger.info('Loading feature_extraction_pipeline_model ...')
    model_paths = args.model_artifact
    feature_extraction_pipeline_model = PipelineModel.load(model_paths)   
       
    logger.info('Loading data ...')
    raw_data_path = args.raw_data_artifact    
    data = spark.read.parquet(raw_data_path).repartition(4)

    logger.info('Preprocessing data ...')     
    data = feature_extraction_pipeline_model.transform(data)

    logger.info('Saving features ...')
    output_artifact_path = args.output_artifact 
    data.write.format('parquet').save(output_artifact_path)

    logger.info('Done')
    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Data Preprocessing'
    )
    # For CLI use 's3a//airflow-cc-output/feature_extraction_pipeline_model'    
    # or 'feature_extraction_pipeline_model'
    parser.add_argument(
        '--model_artifact',
        type=str,
        help='Fully qualified name of feature extraction pipeline model artifact',        
        required=True
    )
    # For CLI use 's3a://airflow-cc-input/test.parquet'
    # or 'test.parquet'
    parser.add_argument(
        '--raw_data_artifact',
        type=str,
        help='Fully qualified name of artifact/dataset for preprocessing',        
        required=True
    )
    # For CLI use 's3a://airflow-cc-output/test_features.parquet'
    # or 'test_features.parquet'
    parser.add_argument(
        '--output_artifact',
        type=str,
        help='Name for preprocessed artefact/dataset folder)',
        required=True
    )
    args = parser.parse_args()
    main(args)