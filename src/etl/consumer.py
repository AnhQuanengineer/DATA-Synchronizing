import json

from kafka import KafkaProducer, KafkaConsumer

def consumer():
    consumer = KafkaConsumer(
        'quandz',
        bootstrap_servers='localhost:9092',
        group_id='group',
        auto_offset_reset='latest',  # Start from earliest messages if no offset
        enable_auto_commit=False
    )
    total_message_consumer = 0
    running = True
    # data_consumer = []
    while running:
        msg_pack = consumer.poll(timeout_ms=500)
        data_consumer = []
        for tp, messages in msg_pack.items():
            for message in messages:
                # print(message)
                data = message.value.decode("utf-8")
                data_consumer.append(json.loads(data))
                total_message_consumer += 1
                # print(f"--------------{total_message_consumer}----------------------")
                yield total_message_consumer, data_consumer

def main():

    for count_consumer, data_consumer in consumer():
        print(f"------consumer message: {count_consumer}--------------")
        print(f"------consumer data: {data_consumer}--------------")

if __name__ == "__main__":
    main()