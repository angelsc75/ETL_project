from confluent_kafka import Producer, Consumer, KafkaError

# Kafka broker address
kafka_broker = "localhost:29092"

# Produce a message to Kafka
producer = Producer({'bootstrap.servers': kafka_broker})
producer.produce('my-topic', key='mykey', value='Hello, Kafka!')
producer.flush()
print("Message produced to Kafka")

# Consume messages from Kafka
consumer = Consumer({
    'bootstrap.servers': kafka_broker,
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['my-topic'])

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
    print(f"Consumed message: key={msg.key().decode('utf-8')}, value={msg.value().decode('utf-8')}")

consumer.close()