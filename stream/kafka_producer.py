import json
import logging
import date
from pykafka import KafkaClient
from . import build_stream

# Set up logging


client = KafkaClient(use_greenlets=True)
topic = client.topics['tweets']
producer = topic.get_sync_producer()

def send_message_to_kafka(producer, key, message):
    """
    :param producer: pykafka producer
    :param key: key to decide partition
    :param message: json serializable object to send
    :return:
    """

    data = json.dumps(message)
    try:
        producer.produce(data, partition_key=f'{key}')
    except Exception as e:



def main():
    client = KafkaClient()
    rules = build_stream.get_rules()
    deleted_rules = build_stream.delete_all_rules()
    set_rules = build_stream.set_rules()
    twitter_stream = get_stream(set_rules)

    with 