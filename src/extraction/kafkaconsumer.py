from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Configuración del consumidor de Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',  # Dirección del broker de Kafka
    'group.id': 'my-group',                 # ID del grupo de consumidores
    'auto.offset.reset': 'earliest'         # Consumir desde el principio si no hay offset
}

# Conectar a MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Reemplaza con la URI de MongoDB si es necesario
db = client.hr_data  # Base de datos
collection = db.hr_data  # Colección

# Crear el consumidor Kafka
consumer = Consumer(consumer_conf)
consumer.subscribe(['probando'])  # Reemplaza con el nombre del tópico de Kafka

# Consumir y almacenar en MongoDB
try:
    while True:
        msg = consumer.poll(1.0)  # Polling para mensajes

        if msg is None:
            continue  # Si no hay mensaje, sigue esperando

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Fin de la partición
                print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            else:
                print(f"Error: {msg.error()}")
                break
        else:
            # Procesar el mensaje consumido
            data = msg.value().decode('utf-8')  # Decodifica el mensaje como texto (UTF-8)
            print(f"Received message: {data} from topic: {msg.topic()} partition: {msg.partition()} offset: {msg.offset()}")

            # Insertar el mensaje en la colección de MongoDB
            document = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "data": data  # Almacena el mensaje como un campo en el documento
            }
            collection.insert_one(document)  # Inserta el documento en MongoDB
            print("Inserted into MongoDB")

finally:
    # Cerrar el consumidor
    consumer.close()
