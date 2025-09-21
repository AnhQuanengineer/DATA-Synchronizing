import json
import threading
import queue
import time
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
                 "FROM user_log_after")
        if last_timestamp:
            query += f" WHERE DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') > '{last_timestamp}'"
        cursor.execute(query)

        rows = cursor.fetchall()
        connection.commit()

        schema = ["user_id", "login", "gravatar_id", "avatar_url", "url", "state", "log_timestamp"]
        data = [dict(zip(schema, row)) for row in rows]
        new_timestamp = max((row["log_timestamp"] for row in data), default=last_timestamp) if data else last_timestamp
        return data, new_timestamp
    except Exception as e:
        print(f"----------Error as: {e}--------------------")
        return [], last_timestamp

def producer(producer_queue):
    config = get_database_config()
    last_timestamp = None
    total_message_producer = 0

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    with MySQLConnect(config['mysql'].host, config["mysql"].port, config["mysql"].user,
                     config["mysql"].password) as mysql_client:
        while True:
            data, new_timestamp = get_data_trigger(mysql_client, last_timestamp)
            last_timestamp = new_timestamp
            for record in data:
                total_message_producer += 1
                producer.send("quandz", record)
                producer_queue.put((total_message_producer, record))  # Send data to queue
                producer.flush()  # may be drop this flush()
def consumer(consumer_queue):
    consumer = KafkaConsumer(
        'quandz',
        bootstrap_servers='localhost:9092',
        group_id='group',
        auto_offset_reset='earliest',
        enable_auto_commit=True
        # auto_offset_reset='latest',  # just read the latest message
        # enable_auto_commit=False
    )
    total_message_consumer = 0

    while True:
        msg_pack = consumer.poll(timeout_ms=500)
        for tp, messages in msg_pack.items():
            for message in messages:
                total_message_consumer += 1
                data = json.loads(message.value.decode("utf-8"))
                consumer_queue.put((total_message_consumer, data))  # Send data to queue

def validate_kafka(producer_data, consumer_data):
    different = [item for item in producer_data if item not in consumer_data]
    if different:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            enable_idempotence=True
        )
        for data in different:
            producer.send("quandz", data)
            print(f"----The diff: {data}-------")
            producer.flush()
    print("-----Validate pass-------------")

def main():
    # Create queue for store data from producer and consumer
    producer_queue = queue.Queue()
    consumer_queue = queue.Queue()

    # Tạo và khởi động thread cho producer và consumer
    producer_thread = threading.Thread(target=producer, args=(producer_queue,))
    consumer_thread = threading.Thread(target=consumer, args=(consumer_queue,))
    producer_thread.daemon = True  # Thread just drop when the script down
    consumer_thread.daemon = True
    producer_thread.start()
    consumer_thread.start()

    while True:
        try:
            # take data to print for consumer and producer
            producer_count, producer_data = producer_queue.get(timeout=5)
            consumer_count, consumer_data = consumer_queue.get(timeout=5)

            print(f"------Message from producer: {producer_count}--------------")
            print(f"------Data from producer: {producer_data}--------------")
            print(f"------Message from consumer: {consumer_count}--------------")
            print(f"------Data from consumer: {consumer_data}--------------")

            # Validate data
            if producer_count == consumer_count:
                validate_kafka([producer_data], [consumer_data])
            else:
                print("--------Missing data--------")
                validate_kafka([producer_data], [consumer_data])

            time.sleep(0.1)

        except queue.Empty:
            print("Waiting for data...")
            time.sleep(1)

# def main():
#     producer_queue = queue.Queue()
#     consumer_queue = queue.Queue()
#
#     producer_thread = threading.Thread(target=producer, args=(producer_queue,))
#     consumer_thread = threading.Thread(target=consumer, args=(consumer_queue,))
#     producer_thread.daemon = True
#     consumer_thread.daemon = True
#     producer_thread.start()
#     consumer_thread.start()
#
#     producer_data_buffer = []
#     consumer_data_buffer = []
#
#     while True:
#         try:
#             # take data from producer
#             while not producer_queue.empty():
#                 producer_count, producer_data = producer_queue.get()
#                 producer_data_buffer.append((producer_count, producer_data))
#
#             # Take data from consumer
#             while not consumer_queue.empty():
#                 consumer_count, consumer_data = consumer_queue.get()
#                 consumer_data_buffer.append((consumer_count, consumer_data))
#
#             # Đồng bộ hóa and validate
#             if producer_data_buffer and consumer_data_buffer:
#                 producer_count, producer_data = producer_data_buffer.pop(0)
#                 consumer_count, consumer_data = consumer_data_buffer.pop(0)
#                 print(f"------Message from producer: {producer_count}--------------")
#                 print(f"------Data from producer: {producer_data}--------------")
#                 print(f"------Message from consumer: {consumer_count}--------------")
#                 print(f"------Data from consumer: {consumer_data}--------------")
#
#                 if producer_count == consumer_count:
#                     validate_kafka([producer_data], [consumer_data])
#                 else:
#                     print("--------Missing data--------")
#                     validate_kafka([producer_data], [consumer_data])
#
#             time.sleep(0.1)
#
#         except queue.Empty:
#             print("Waiting for data...")
#             time.sleep(1)

if __name__ == "__main__":
    main()