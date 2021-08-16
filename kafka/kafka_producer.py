import json
import logging
import time
from pykafka import KafkaClient
from . import build_stream

# Set up logging


client = KafkaClient(use_greenlets=True)
topic = client.topics['tweets']
producer = topic.get_sync_producer()

def send_message_to_kafka(producer, message):
    """
    :param producer: pykafka producer
    :param key: key to decide partition
    :param message: json serializable object to send
    :return:
    """

    #data = json.dumps(message)
    try:
        start = time.time()
        producer.produce(message)
        logging.info(u'Time take to push to Kafka: {}'.format(time.time() - start))
    except Exception as e:
        logging.exception(e)
        pass # for at least once delivery you will need to catch network errors and retry.
