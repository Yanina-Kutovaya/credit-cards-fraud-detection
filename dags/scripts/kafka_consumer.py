#!/usr/bin/env python
"""Credit cards fraud detection project kafka consumer"""

import json
import argparse
from kafka import KafkaConsumer

import pandas as pd
from custom_transformers import (
    DiscreteToBinaryTransformer,
    ContinuousOutliersCapper,
    TimeFeaturesGenerator,
    ScalarNAFiller,
    StringFromDiscrete
)
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.pipeline import PipelineModel


def main():
    argparser = argparse.ArgumentParser(description=__doc__)
    argparser.add_argument(
        '-g', 
        '--group_id',
        default='group_1',
        required=True,
        help='kafka consumer group_id'
    )
    argparser.add_argument(
        '-b',
        '--bootstrap_server',
        default='rc1a-jn0p20gm58106tdp.mdb.yandexcloud.net:9091',
        help='kafka server address:port'
    )
    argparser.add_argument(
        '-t', '--topic', default='inference_1', help='kafka topic to consume'
    )
    argparser.add_argument(
        '--inference_artifact',        
        default='Spark_GBTClassifier_v1',
        type=str,
        help='Name for the serialized model (Inference Artifact folder)'        
    )
    args = argparser.parse_args()

    consumer = KafkaConsumer(
        bootstrap_servers=[args.bootstrap_server],
        group_id=args.group_id,
        value_deserializer=json.loads,
    )
    consumer.subscribe(topics=[args.topic])

    make_predictions(consumer)


def make_predictions(consumer):
    APP_NAME = 'Inference'
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()          
    model = PipelineModel.load(args.inference_artifact)
    results = []

    def extract_prob(v):
        try:
            return float(v[1])  
        except ValueError:
            return None

    extract_prob_udf = F.udf(extract_prob, DoubleType())

    print('Waiting for a new data. Press Ctrl+C to stop')
    cols = ['TransactionID', 'card1', 'isFraud', 'probability']     
    count = 0    
    try:
        for msg in consumer:
            print(
                f'{msg.topic}:{msg.partition}:{msg.offset}: key={msg.key}'
            )
            df = spark.createDataFrame(pd.read_json(msg.value)) 
            predictions = model.transform(df).select(cols)
            predictions = (
                predictions
                .withColumn('prob', extract_prob_udf(F.col('probability')))
                .select(['TransactionID', 'card1', 'isFraud', 'prob'])
                .toPandas().loc[0, :]
                .tolist()
            )
            results.append(predictions)
            count += 1
    except KeyboardInterrupt:
        pass

    cards_anti_rating = (
    spark.createDataFrame(results, cols)    
    .groupBy('card1').max('probability')
    .orderBy(F.col('max(probability)').desc())
    .select('card1', 'max(probability)')
    .toPandas()
    )
    print(cards_anti_rating)
    print(f'Total {count} transactions received')

if __name__ == '__main__':
    main()
