APP_NAME = 'MLFlow and Automated Retraining'
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output'
YC_SOURCE_BUCKET = 'airflow-cc-source'

import boto3
import logging
import argparse
from datetime import datetime
from pyspark import SparkFiles
from pyspark.sql import SparkSession

import mlflow
from mlflow.tracking import MlflowClient
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from custom_transformers import (
    DiscreteToBinaryTransformer,
    ContinuousOutliersCapper,
    TimeFeaturesGenerator,
    ScalarNAFiller,
    StringFromDiscrete
)
from fraud_detection_model_pipeline import get_fraud_detection_model_pipeline


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def main(args):
    # Create Spark Session
    logger.info('Creating Spark Session ...')
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
      
    # Load data
    logger.info('Loading data ...')    
    data_path = args.train_artifact
    data = spark.read.parquet(data_path, header=True, inferSchema=True) 
    
    # Prepare MLflow experiment for logging
    client = MlflowClient()
    experiment_id = client.create_experiment(args.output_artifact)
    # Set run_name for search in mlflow 
    run_name = f'Credit_cards_fraud_detection_pipline {str(datetime.now())}' 
    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        inf_pipeline = get_fraud_detection_model_pipeline()
        classifier = inf_pipeline.getStages()[-1]
        paramGrid = (
            ParamGridBuilder()
            .addGrid(classifier.maxDepth, [5, 10, 15]) 
            .addGrid(classifier.minInstancesPerNode, [15, 100, 200])               
            .build()
        )
        evaluator = BinaryClassificationEvaluator(
            labelCol='isFraud',
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        ) 
        # By default 80% of the data will be used for training, 20% for validation.
        trainRatio = 1 - args.val_frac

        # A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.  
        tvs = TrainValidationSplit(
            estimator=inf_pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=evaluator,
            trainRatio=trainRatio,
            parallelism=2
        )
        # Run TrainValidationSplit, and choose the best set of parameters.
        logger.info("Fitting new inference pipeline ...")
        model = tvs.fit(data)

        # Log params, metrics and model with MLFlow
        run_id = mlflow.active_run().info.run_id
        logger.info(f"Logging optimal parameters to MLflow run {run_id} ...")

        best_maxDepth = model.bestModel.stages[-1].getMaxDepth() 
        best_minInstancesPerNode = model.bestModel.stages[-1].getMinInstancesPerNode()    

        logger.info(model.bestModel.stages[-1].explainParam('maxDepth'))       
        logger.info(model.bestModel.stages[-1].explainParam('minInstancesPerNode')) 

        mlflow.log_param('maxDepth', best_maxDepth)
        mlflow.log_param('minInstancesPerNode', best_minInstancesPerNode)           
        
        logger.info('Scoring the model ...')  
        predictions = model.transform(data)
        roc_auc = evaluator.evaluate(predictions)

        logger.info(f'Logging metrics to MLflow run {run_id} ...')    
        mlflow.log_metric('roc_auc', roc_auc)
        logger.info(f'Model roc_auc: {roc_auc}')

        logger.info("Saving pipeline ...")
        mlflow.spark.save_model(model, args.output_artifact)

        logger.info("Exporting/logging pipline ...")
        mlflow.spark.log_model(model, args.output_artifact)  

        logger.info('Done')

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Model (Inference Pipline) Training'
    )
    # For CLI use f's3a://airflow-cc-input/train.csv'
    parser.add_argument(
        "--train_artifact", 
        type=str,
        help='Fully qualified name for training artifact/dataset' 
        'Training dataset will be split into train and validation',
        required=True
    )
    parser.add_argument(
        "--val_frac",
        type=float,
        default = 0.2,
        help="Size of the validation split. Fraction of the dataset.",
    )
    # For CLI use 'Spark_GBTClassifier_v1'
    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the output serialized model (Inference Artifact folder)",
        required=True,
    )
    args = parser.parse_args()
    main(args)