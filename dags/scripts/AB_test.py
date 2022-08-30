APP_NAME = 'Model Automated Retraining and Validation'
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output'
YC_SOURCE_BUCKET = 'airflow-cc-source'

import logging
import argparse
import mlflow
import pandas as pd
import numpy as np
from scipy.stats import norm, ttest_ind

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier

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

def train_validation_split(data):
    t = 3600 * 24 * 30
    train = data.filter(F.col('TransactionDT') < t)
    compromised_cards = (
        train['card1', 'isFraud'].filter(F.col('isFraud') == 1)
        .groupBy('card1').count().toPandas()['card1'].tolist()
    )           
    valid = (
        data.filter(F.col('TransactionDT') >= t)
        .where(~F.col('card1').isin(compromised_cards))
    )
    return train, valid


def get_cards_precision_top_k(predictions, k=50, bootstrap_iterations=100, seed=25):
    np.random.seed(seed)
    scores = pd.DataFrame(
        data={f'precision_top_{k}': 0.0}, 
        index=range(bootstrap_iterations)
    )
    def extract_prob(v):
        try:
            return float(v[1])  
        except ValueError:
            return None

    extract_prob_udf = F.udf(extract_prob, DoubleType())
    predictions = predictions.withColumn(
        'prob_flag', extract_prob_udf(F.col('probability'))
    )
    df = (
        predictions.select('card1', 'prob_flag', 'isFraud')
        .groupBy('card1').max('prob_flag', 'isFraud')
        .orderBy(F.col('max(prob_flag)').desc())
        .select('max(isFraud)')
        .limit(k)        
        .toPandas()    
    )
    for i in range(bootstrap_iterations):
        sample = df.sample(frac=1.0, replace=True)
        scores.loc[i, f'precision_top_{k}'] = sample.sum()[0] / k

    precision_mean = scores[f'precision_top_{k}'].mean()
    precision_std = scores[f'precision_top_{k}'].std()
    precision_low = precision_mean - 3 * precision_std
    precision_upp = precision_mean + 3 * precision_std
    logger.info(f'CP@{k} = {round(precision_mean, 4)} +/- {round(precision_std, 4)}\n')    

    return scores


def main(args):
    logger.info('Creating Spark Session ...')
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    logger.info('Loading data ...')
    data_path = args.train_artifact
    data = spark.read.parquet(data_path, header=True, inferSchema=True)

    logger.info('Train validation split ...')
    train, valid = train_validation_split(data)

    logger.info('Loading saved model ...')
    saved_model_path = args.saved_artifact
    loaded_model = mlflow.spark.load_model(saved_model_path)

    logger.info('Scoring saved model ...') 
    predictions_A = loaded_model.transform(valid)
    scores_A = get_cards_precision_top_k(predictions_A)

    logger.info("Building new inference pipeline ...")
    inf_pipeline = get_fraud_detection_model_pipeline()
    classifier_B = GBTClassifier(
            featuresCol='features',
            labelCol='isFraud',
            maxDepth = 6,
            minInstancesPerNode=1000
    )
    stages = inf_pipeline.getStages()[:-1]
    stages.append(classifier_B)
    new_pipeline = Pipeline(stages=stages)

    logger.info("Fitting new inference pipeline ...")
    model_B = new_pipeline.fit(train)

    logger.info('Scoring model B ...')  
    predictions_B = model_B.transform(valid)
    scores_B = get_cards_precision_top_k(predictions_B)

    logger.info('Performing A/B test ...')
    k = args.k_cards    
    pvalue = ttest_ind(
        scores_A[f'precision_top_{k}'], 
        scores_B[f'precision_top_{k}'], 
        alternative="less"
    ).pvalue
    logger.info(f'pvalue: {pvalue: g}')    
    if pvalue < args.p_value:
        logger.info('Reject null hypothesis.')        
        logger.info("Saving inference pipeline B ...")        
        model_B.save(args.output_artifact)     
    else:
        logger.info('Accept null hypothesis.')       


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Model (Inference Pipline) Training and Validation'
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
        "--k_cards",
        type=int,
        default = 50,
        help="Number of cards for precision top k calculation.",
    )
    parser.add_argument(
        "--p_value",
        type=float,
        default = 0.01,
        help="p-value for A/B test model selection.",
    )
    # For CLI use 'Spark_GBTClassifier_v1'
    parser.add_argument(
        "--saved_artifact",
        type=str,
        help="Name for the saved serialized model (Inference Artifact folder)",
        required=True,
    )
    # For CLI use 'Spark_GBTClassifier_v2'
    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the output serialized model (Inference Artifact folder)",
        required=True,
    )
    args = parser.parse_args()
    main(args)
