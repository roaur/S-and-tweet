from stream import build_stream
from kafka import kafka_producer
import logging
import requests
from pykafka import KafkaClient

client = KafkaClient(hosts="localhost:9092")
topic = client.topics['tweets']
producer = topic.get_sync_producer()


def get_stream(set):
    logging.info('starting get_stream()')
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=build_stream.bearer_oauth, stream=True,
    )
    logging.info(f'get_stream response: {response.status_code}')

    if response.status_code != 200:
        err = "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
        )
        logging.error(err)
        raise Exception(err)

    for response_line in response.iter_lines():
        if response_line:
            kafka_producer.send_message_to_kafka(producer, response_line)


def main():
    rules = build_stream.get_rules()
    delete = build_stream.delete_all_rules(rules)
    set = build_stream.set_rules(delete)
    get_stream(set)


if __name__ == "__main__":
    main()
