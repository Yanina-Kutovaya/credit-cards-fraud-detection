APP_NAME = 'MLFlow and Automated Retraining'
YC_SOURCE_BUCKET = 'airflow-cc-source'

import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession

import mlflow
from mlflow.tracking import MlflowClient

from custom_transformers import (
    DiscreteToBinaryTransformer,
    ContinuousOutliersCapper,
    TimeFeaturesGenerator,
    ScalarNAFiller,
    StringFromDiscrete
)
from fraud_detection_model_pipeline import get_fraud_detection_model_pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def get_data_path(train_artifact_path):
    data_path = train_artifact_path
    return data_path

def main(args):
    # Create Spark Session
    logger.info('Creating Spark Session ...')
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
  
    # Load data
    logger.info('Loading data ...')
    train_artifact_name = args.train_artifact
    data_path = get_data_path(train_artifact_name)
    data = (
        spark.read.format('csv')
        .option(header='true', inferSchema='true')
        .load(data_path)
    )
    # Prepare MLflow experiment for logging
    client = MlflowClient()
    experiment = client.get_experiment_by_name('Spark_Experiment')
    experiment_id = experiment.experiment_id

    # Set run_name for search in mlflow 
    run_name = f'Fraud_detection_model_pipline {str(datetime.now())}' 

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        inf_pipline = get_fraud_detection_model_pipeline()

        logger.info('Fitting new model / Inference pipline ...')
        model = inf_pipline.fit(data)

        logger.info('Scoring the model ...')
        evaluator = BinaryClassificationEvaluator(
            labelCol=TARGET_COLUMN[0],
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        predictions = model.transform(data)
        roc_auc = evaluator.evaluate(predictions)

        run_id = mlflow.active_run().info.run_id
        logger.info(f'Logging metrics to MLflow run {run_id} ...')    
        mlflow.log_metric('roc_auc', roc_auc)
        logger.info(f'Model roc_auc: {roc_auc}')

        logger.info('Saving the model ...')
        mlflow.spark.save_model(model, args.output_artifact)

        logger.info('Exporting / logging model ...')
        mlflow.spark.log_model(model, args.output_artifact)

        logger.info('Done')

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