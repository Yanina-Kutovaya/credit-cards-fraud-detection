#!/usr/bin/env python
"""Credit cards fraud detection project kafka producer"""

import json
from typing import Dict, NamedTuple
import logging
import random
import datetime
import argparse
from collections import namedtuple
import kafka

import pyspark
from pyspark.sql import SparkSession


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    offset: int


def main():
    argparser = argparse.ArgumentParser(description=__doc__)
    argparser.add_argument(
        '-b',
        '--bootstrap_server',
        default='rc1a-jn0p20gm58106tdp.mdb.yandexcloud.net:9091',
        help='Kafka server address:port',
    )
    argparser.add_argument(
        '-t', 
        '--topic', 
        default='inference_1',         
        help='Kafka topic to consume'
    )
    argparser.add_argument(
        '-n',
        default=10,
        type=int,
        help='Number of messages to send',
    )
    argparser.add_argument(
        '--data_artifact',        
        default='test.parquet',
        type=str, 
        help='Fully qualified name for testing dataset'
    )    
    
    args = argparser.parse_args()

    producer = kafka.KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=serialize,
    )
    APP_NAME = 'Generate test data'
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()    
    data = spark.read.parquet(
        args.data_artifact, header=True, inferSchema=True
    )
    try:
        for i in range(args.n):
            record_md = send_message(data, producer, args.topic)
            print(
                f'Msg sent. Topic: {record_md.topic}, partition:{record_md.partition}, offset:{record_md.offset}'
            )
    except kafka.errors.KafkaError as err:
        logging.error(err)
    producer.flush()
    producer.close()


def send_message(data, producer: kafka.KafkaProducer, topic: str) -> RecordMetadata:
    generated_data = generate_data(data)
    features = producer.send(
        topic=topic,
        key=str(generated_data['card_id']).encode('ascii'),
        value=generated_data,
    )
    # Block for 'synchronous' sends
    record_metadata = features.get(timeout=1)
    return RecordMetadata(
        topic=record_metadata.topic,
        partition=record_metadata.partition,
        offset=record_metadata.offset,
    )


def generate_data(data) -> Dict:
    df = data.sample(0.00003).toPandas()
    card_id = df.loc[0, 'card1']
    card_data = df.iloc[:1, :].to_json()
    return {
        'ts': datetime.datetime.now().isoformat(),        
        'card_id': card_id,
        'features': card_data, 
    }


def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode('utf-8')


if __name__ == '__main__':
    main()