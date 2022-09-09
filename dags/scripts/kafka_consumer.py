#!/usr/bin/env python
"""Credit cards fraud detection project kafka consumer"""

import json
import argparse
import logging
import datetime
from typing import Dict, NamedTuple
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

import pandas as pd
from custom_transformers import (
    DiscreteToBinaryTransformer,
    ContinuousOutliersCapper,
    TimeFeaturesGenerator,
    ScalarNAFiller,
    StringFromDiscrete
)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.ml.pipeline import PipelineModel


def main():
    argparser = argparse.ArgumentParser(description=__doc__)
    argparser.add_argument(
        '-g', 
        '--group_id',
        default='group_1',        
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
        '-t2', '--topic_2', default='results', help='kafka topic for inference results'
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
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username='user_2',
        sasl_plain_password='Volgograd38',        
        ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexCA.crt",
        value_deserializer=json.loads,
    )
    consumer.subscribe(topics=[args.topic])

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],        
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_username='user_2',
        sasl_plain_password='Volgograd38',
        ssl_cafile='/usr/local/share/ca-certificates/Yandex/YandexCA.crt',
        value_serializer=serialize,
    )

    make_predictions(consumer, producer, args.inference_artifact, args.topic_2)
    

def make_predictions(consumer, producer, inference_artifact_path, topic_2):
    APP_NAME = 'Inference'
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()          
    model = PipelineModel.load(inference_artifact_path)
    
    print('Waiting for a new data. Press Ctrl+C to stop')        
    count_1 = 0
    count_2 = 0    
    try:
        for msg in consumer:
            ### Generate prediction
            print(
                f'{msg.topic}: {msg.partition}: {msg.offset}: key={msg.key}'
            )
            df = pd.read_json(msg.value['features'])            
            df = spark.createDataFrame(df) 
            predictions = model.transform(df)

            def extract_prob(v):
                try:
                    return float(v[1])  
                except ValueError:
                    return None

            extract_prob_udf = F.udf(extract_prob, DoubleType())
            cols = ['TransactionID', 'card1', 'probability_']            
            predictions = (
                predictions
                .withColumn('probability_', extract_prob_udf(F.col('probability')))
                .select(cols)
                .toPandas()
            )
            card_id = int(predictions.loc[0, 'card1'])
            prediction = predictions.loc[0, cols].to_json()            
            print(prediction)
            count_1 += 1
            ### Send results 
            try:
                record_md = send_message(card_id, prediction, producer, topic_2)
                print(
                    f'Msg sent. Topic: {record_md.topic}, partition:{record_md.partition}, offset:{record_md.offset}'
                )
            except KafkaError as err:
                logging.error(err)
            count_2 += 1
        producer.flush()
    except KeyboardInterrupt:
        pass

    print(f'\nTotal {count_1} transactions received and {count_2} predictions sent')


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    offset: int


def send_message(card_id, predictions, producer: KafkaProducer, topic_2: str) -> RecordMetadata:
    generated_prediction = generate_prediction(card_id, predictions)
    message = producer.send(
        topic=topic_2,
        key=str(card_id).encode('ascii'),
        value=generated_prediction,
    )
    # Block for 'synchronous' sends
    record_metadata = message.get(timeout=1)
    return RecordMetadata(
        topic=record_metadata.topic,
        partition=record_metadata.partition,
        offset=record_metadata.offset,
    )


def generate_prediction(card_id, predictions) -> Dict:    
    return {
        'ts': datetime.datetime.now().isoformat(),        
        'card_id': card_id,
        'result': predictions, 
    }


def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode('utf-8')


if __name__ == '__main__':
    main()
