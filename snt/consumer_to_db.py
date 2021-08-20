from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
from psycopg2.extras import Json

consumer = KafkaConsumer(
    'tweets',
    bootstrap_servers=['analysis.somewhatspatial.lan:9092'],
    #auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
)

pg_con = psycopg2.connect("host=100.100.100.42 dbname=datascience user=roman")

with pg_con.cursor() as cursor:
    for message in consumer:
        time = datetime.fromtimestamp(message.timestamp/1000)
        message = json.loads(message.value)

        cursor.execute(f"insert into snt.raw_msg (data, msg_time) values ({Json(message)}, '{time}')")
        pg_con.commit()