import requests
import os
import json
import logging
from pykafka import KafkaClient


bearer_token = os.environ.get("BEARER_TOKEN")

logging.basicConfig(filename='example.log', filemode='w', level=logging.DEBUG)

client = KafkaClient(hosts='localhost:9092')
topic = client.topics['tweets']
producer = topic.get_sync_producer()


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "S-n-Tweet Alpha"
    return r


def get_rules():
    logging.info('starting get_rules()')
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        err = "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        logging.error(err)
        raise Exception(
            err
        )
    rule_response = response.json()
    logging.info('done get_rules()')
    logging.info(f'got rules: {rule_response}')
    return rule_response


def delete_all_rules(rules):
    logging.info('starting delete_all_rules()')
    
    if rules is None or "data" not in rules:
        return None
        logging.info('no existing rules found')

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        err =  "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        logging.error(err)
        raise Exception(
            err
        )
    logging.info('done delete_all_rules()')
    #print(json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    logging.info('starting set_rules()')
    rules = [
        {"value": "TSLA"},
        {"value": "MSFT"}, 
        {"value": "GOOG"}, 
        #{"value": "BTC"}, 
        #{"value": "#ElectionsCanada"}, 
        #{"value": "AAPL"}, 
        #{"value": "AMZN"}, 
    ]
    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    logging.info(f'set rules: {json.dumps(response.json())}')
    if response.status_code != 201:
        err = "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        logging.error(err)
        raise Exception(
            err
        )
    logging.info('done setting rules')
    #return response.json()
    #print(json.dumps(response.json(), indent=4, sort_keys=True))


def get_stream(set):
    logging.info('starting get_stream()')
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
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
            #json_response = json.loads(response_line)
            kafka_producer.send_message_to_kafka(producer, response_line)
            #print(json.dumps(json_response, indent=4, sort_keys=True))

def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set)
    #get_stream(rules)


if __name__ == "__main__":
    main()