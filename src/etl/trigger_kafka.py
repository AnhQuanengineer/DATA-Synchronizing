import json

from kafka import KafkaProducer, KafkaConsumer
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.database_config import get_database_config
from database.mysql_connect import MySQLConnect


def get_data_trigger(mysql_client, last_timestamp):
    try:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        database = "github_data"
        connection.database = database

        query = ("SELECT user_id, login, gravatar_id, avatar_url, url, state, "
                 " DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') AS log_timestamp1 "
                 "FROM user_log_after"
                 )
        if last_timestamp:
            # query += " WHERE DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s') = DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')"
            query += f" WHERE DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') > '{last_timestamp}'"
            cursor.execute(query)
        else:
            cursor.execute(query)

        rows = cursor.fetchall()
        connection.commit()

        schema = ["user_id", "login", "gravatar_id", "avatar_url", "url", "state", "log_timestamp"]
        data = [dict(zip(schema, row)) for row in rows]

        #get last_timestamp lastest
        new_timestamp = max((row["log_timestamp"] for row in data), default=last_timestamp) if data else last_timestamp
        return data, new_timestamp
    except Exception as e:
        print(f"----------Error as : {e}--------------------")
        return [], last_timestamp


def producer():
    config = get_database_config()
    last_timestamp = None
    total_message_producer = 0

    while True:
        with MySQLConnect(config['mysql'].host, config["mysql"].port, config["mysql"].user,
                          config["mysql"].password) as mysql_client:

            # data_producer = []
            # send to kafka
            producer = KafkaProducer(bootstrap_servers='localhost:9092'
                                     , value_serializer=lambda v: json.dumps(v).encode('utf-8')
                                    )
            while True:
                data, new_timestamp = get_data_trigger(mysql_client, last_timestamp)
                last_timestamp = new_timestamp
                # data_producer = []
                for record in data:
                    data_producer = []
                    # time.sleep(1)
                    producer.send("quandz", record)
                    total_message_producer += 1
                    producer.flush()
                    # print(record)
                    # print(f"--------------{total_message_producer}----------------------")
                    data_producer.append(record)
                    yield total_message_producer, data_producer

def consumer():
    consumer = KafkaConsumer(
        'quandz',
        bootstrap_servers='localhost:9092',
        group_id='group',
        auto_offset_reset='earliest',  # Start from earliest messages if no offset
        enable_auto_commit=True
    )
    total_message_consumer = 0
    running = True
    # data_consumer = []
    while running:
        msg_pack = consumer.poll(timeout_ms=500)
        # data_consumer = []
        for tp, messages in msg_pack.items():
            for message in messages:
                data_consumer = []
                data = message.value.decode("utf-8")
                data_consumer.append(json.loads(data))
                total_message_consumer += 1
                # print(f"--------------{total_message_consumer}----------------------")
                yield total_message_consumer, data_consumer

def validate_kafka(data_producer, data_consumer):
    different = [item for item in data_producer if item not in data_consumer]
    if different:
        producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'), enable_idempotence=True)

        for data in different:
            producer.send("quandz", data)
            print(f"----The diff : {different}-------")
            producer.flush()
    print("-----validate pass-------------")
def main():

    # count_messages_producer, data_producer = next(producer())
    # count_messages_consumer, data_consumer = next(consumer())

    for (count_messages_producer, data_producer), (count_messages_consumer, data_consumer) in zip(producer(),
                                                                                                  consumer()):
        print(f"------Tin nhắn từ producer: {count_messages_producer}--------------")
        print(f"------Dữ liệu producer: {data_producer}--------------")
        print(f"------Tin nhắn từ consumer: {count_messages_consumer}--------------")
        print(f"------Dữ liệu consumer: {data_consumer}--------------")

        if count_messages_producer == count_messages_consumer:
            validate_kafka(data_producer, data_consumer)
        else:
            print("--------missing data--------")
            validate_kafka(data_producer, data_consumer)

    # for count_messages_producer, data_producer in producer():
    #     print(f"------Tin nhắn từ producer: {count_messages_producer}--------------")
    #     print(f"------Dữ liệu producer: {data_producer}--------------")


if __name__ == "__main__":
    main()

#trigger mysql to kafka