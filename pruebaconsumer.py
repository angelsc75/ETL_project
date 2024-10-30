from confluent_kafka import Producer, Consumer, KafkaError
import os

# Producer
producer = Producer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')})
producer.produce('probando', key='key', value='hello world')
producer.flush()

# Consumer
consumer = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['probando'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Consumer error: {msg.error()}")
            break
    print(f"Consumed message: {msg.key().decode('utf-8')} - {msg.value().decode('utf-8')}")

consumer.close()