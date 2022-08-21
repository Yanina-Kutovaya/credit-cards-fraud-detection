APP_NAME = 'MLFlow and Automated Retraining'
YC_SOURCE_BUCKET = 'airflow-cc-source'

import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassifier

import mlflow
from custom_transformers import (
    DiscreteToBinaryTransformer,
    ContinuousOutliersCapper,
    TimeFeaturesGenerator,
    ScalarNAFiller,
    StringFromDiscrete
)
from feature_extraction_pipeline import get_feature_extraction_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def get_data_path(train_artifact_path):
    data_path = train_artifact_path
    return data_path


def get_classifier():
    classifier = GBTClassifier(
        labelCol=TARGET_COLUMN[0], featuresCol='features', 
      maxDepth=6, seed=42, minInstancesPerNode=15
    )
    return classifier


def main(args):
    logger.info('Creating Spark Session ...')
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
  
    logger.info('Loading data ...')
    train_artifact_name = args.train_artifact
    data_path = get_data_path(train_artifact_name)
    data = (
        spark.read.format('csv')
        .option(header='true', inferSchema='true')
        .load(data_path)
    )
    feature_extraction_pipeline = get_feature_extraction_pipeline()
    train_data = feature_extraction_pipeline.transform(data)
    classifier = get_classifier()

    logger.info('Fitting the model ...')
    model = classifier.fit(train_data)

    logger.info('Saving the model ...')
    mlflow.spark.save_model(model, args.output_artifact)

    spark.stop()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Model (Inference Pipline) Training'
    )
    # For CLI use f's3a://airflow-cc-input/train.csv'
    parser.add_argument(
        '--train_artifact',
        type=str,
        help='Fully qualified name for traning artifact/dataset'
        'Training dataset will be split into train and validation',
        required=True
    )
    # For CLI use 'Credit_cards_fraud_detection'
    parser.add_argument(
        '--output_artifact',
        type=str,
        help='Name for the output serialized model (Inference Artifact folder)',
        required=True
    )
    args = parser.parse_args()
    main(args)