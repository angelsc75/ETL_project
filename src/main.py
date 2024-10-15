from extraction.kafkaconsumer import KafkaConsumer
from loading.mongodbloader import MongoDBLoader  # Importar MongoDBLoader
import os

if __name__ == "__main__":
    # Configuraciones de Kafka y MongoDB
    kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:29092')
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://adminUser:12345@localhost:27017/hr_data')

    # Inicializa el loader de MongoDB
    mongo_loader = MongoDBLoader(uri=mongo_uri)

    # Inicializa el consumidor de Kafka
    kafka_consumer = KafkaConsumer(kafka_broker, "hrpro-group", mongo_loader)

    # Comienza a consumir mensajes de Kafka y cargarlos en MongoDB
    kafka_consumer.start_consuming()