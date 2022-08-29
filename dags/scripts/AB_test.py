APP_NAME = 'Model Validation'
YC_INPUT_DATA_BUCKET = 'airflow-cc-input'
YC_OUTPUT_DATA_BUCKET = 'airflow-cc-output'
YC_SOURCE_BUCKET = 'airflow-cc-source'

import requests

CUSTOM_TRANSFORMERS_URL = f'https://storage.yandexcloud.net/{YC_SOURCE_BUCKET}/custom_transformers.py'
FRAUD_DETECTION_MODEL_PIPELINE_URL = f'https://storage.yandexcloud.net/{YC_SOURCE_BUCKET}/fraud_detection_model_pipeline.py'
TRAIN_DATA_URL = f'https://storage.yandexcloud.net/{YC_INPUT_DATA_BUCKET}/train.parquet'

r = requests.get(CUSTOM_TRANSFORMERS_URL, allow_redirects=True)
open('custom_transformers.py', 'wb').write(r.content)

r = requests.get(FRAUD_DETECTION_MODEL_PIPELINE_URL, allow_redirects=True)
open('fraud_detection_model_pipeline.py', 'wb').write(r.content)

r = requests.get(TRAIN_DATA_URL, allow_redirects=True)
open('train.parquet', 'wb').write(r.content)


import numpy as np
import pandas as pd
import logging
import argparse
from datetime import datetime
from pyspark import SparkFiles
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType

import mlflow
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier

from scipy.stats import norm
import seaborn as sns

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

def get_train_valid_dataset(data):
    logger.info('Train validation split ...')
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


def get_model(classifier, inf_pipeline, train):
    stages = inf_pipeline.getStages()[:-1]
    stages.append(classifier)
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(train)
  
    return model

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


def main():
    np.random.seed(18)
    # Create Spark Session
    logger.info('Creating Spark Session ...')
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    # Load data
    logger.info('Loading data ...')
    data_path = 'train.parquet'
    data = spark.read.parquet(data_path, header=True, inferSchema=True)

    logger.info('Splitting data into train and validation dataset ...')
    train, valid = get_train_valid_dataset(data)

    inf_pipeline = get_fraud_detection_model_pipeline()
    classifier_B = GBTClassifier(
        featuresCol='features',
        labelCol='isFraud',
        maxDepth = 10,
        minInstancesPerNode=100
    )
    logger.info("Fitting inference pipeline B ...")
    model_B = get_model(classifier_B, inf_pipeline, train)

    logger.info('Scoring model B ...')  
    predictions_B = model_B.transform(valid)
    
    logger.info("A/B testing ...")
    scores_A = pd.read_csv('scores_Spark_GBTClassifier_v1.csv')
    scores_B = get_cards_precision_top_k(predictions_B)

    from scipy.stats import ttest_ind

    k = 50
    alpha = 0.05

    pvalue = ttest_ind(
        scores_A[f'precision_top_{k}'], 
        scores_B[f'precision_top_{k}'], 
        alternative="less"
    ).pvalue

    print(f"pvalue: {pvalue: g}")
    if pvalue < alpha:
        logger.info("Reject null hypothesis.")
        logger.info("Saving inference pipeline B ...")
        mlflow.spark.save_model(model_B, 'Spark_GBTClassifier_v2')
        scores_B.to_csv('scores_Spark_GBTClassifier_v2.csv', index=False) 
    else:
        logger.info("Accept null hypothesis.")


if __name__ == "__main__":
    main()
