import requests
import os
import json
import logging
from logging.handlers import TimedRotatingFileHandler 
import time
from kafka import KafkaProducer
import psycopg2
from datetime import datetime
from psycopg2.extras import Json
from psycopg2.sql import SQL, Literal, Identifier
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Daily rotating logs
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

handler = TimedRotatingFileHandler('snt.log', 
                                   when='midnight',
                                   backupCount=10)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

bearer_token = os.environ.get("BEARER_TOKEN")

http = requests.Session()

# We want to account for timeouts. The Twitter API says there should be 20s
# heartbeat messages as per 
# https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/handling-disconnections
# We will set our timeout limit to 30s which should be able to account
# for the heartbeats (which are newline characters - \n)

DEFAULT_TIMEOUT = 30 # seconds


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

retry_strategy = Retry(
    total=10,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)

http.mount("https://", TimeoutHTTPAdapter(max_retries=retry_strategy))
http.mount("http://", TimeoutHTTPAdapter(max_retries=retry_strategy))

producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "S-n-Tweet Alpha"
    return r


def get_rules():
    logger.info('starting get_rules()')
    response = http.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        err = "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        logger.error(err)
        raise Exception(
            err
        )
    rule_response = response.json()
    logger.info('done get_rules()')
    logger.info(f'got rules: {rule_response}')
    return rule_response


def delete_all_rules(rules):
    logger.info('starting delete_all_rules()')
    
    if rules is None or "data" not in rules:
        return None
        logger.info('no existing rules found')

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = http.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        err =  "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        logger.error(err)
        raise Exception(
            err
        )
    logger.info('done delete_all_rules()')
    #print(json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    logger.info('starting set_rules()')
    rules = [
        {"value": "TSLA"},
        {"value": "MSFT"}, 
        {"value": "GOOG"}, 
        #{"value": "GME"},
        #{"value": "BTC"}, 
        #{"value": "#ElectionsCanada"}, 
        #{"value": "AAPL"}, 
        #{"value": "AMZN"}, 
    ]
    payload = {"add": rules}
    response = http.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    logger.info(f'set rules: {json.dumps(response.json())}')

    j = response.json()

# Example response
#
# {
#   "data": [
#     {
#       "value": "TSLA",
#       "id": "1429130887095017481"
#     },
#     {
#       "value": "GOOG",
#       "id": "1429130887095017480"
#     },
#     {
#       "value": "MSFT",
#       "id": "1429130887095017482"
#     }
#   ],
#   "meta": {
#     "sent": "2021-08-20T20:21:29.534Z",
#     "summary": {
#       "created": 3,
#       "not_created": 0,
#       "valid": 3,
#       "invalid": 0
#     }
#   }
# }
    senttime = datetime.strptime(j['meta']['sent'], '%Y-%m-%dT%H:%M:%S.%fZ')
    summary_created = j['meta']['summary']['created']
    summary_not_created = j['meta']['summary']['not_created']
    summary_valid = j['meta']['summary']['valid']
    summary_invalid = j['meta']['summary']['invalid']

    try:
        with psycopg2.connect("host=100.100.100.42 dbname=datascience user=roman") as pg_con:
            with pg_con.cursor() as cursor:
                for rule in j['data']:
                    match_value = rule['value']
                    match_id = rule['id']

                    sql = """
                        insert into snt.rules 
                        (match_id, match_value, sent_time, summary_created, summary_not_created, summary_valid, summary_invalid) 
                        values 
                        (%s, %s, %s, %s, %s, %s, %s);
                        """

                    cursor.execute(
                            sql,
                            (match_id, match_value, str(senttime), summary_created, summary_not_created, summary_valid, summary_invalid)
                        )
                    pg_con.commit()
    except Exception as e:
        logger.error(e)
        raise e

    if response.status_code != 201:
        err = "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        logger.error(err)
        raise Exception(
            err
        )
    logger.info('done setting rules')


def get_stream(set):
    logger.info('starting get_stream()')
    response = http.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    logger.info(f'get_stream response: {response.status_code}')

    if response.status_code != 200:
        err = "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
        )
        logger.error(err)
        raise Exception(err)

    for response_line in response.iter_lines():
        if response_line:
            producer.send('tweets', response_line, timestamp_ms=int(time.time()*1000))

def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set)


if __name__ == "__main__":
    main()
