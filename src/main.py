from extraction.kafkaconsumer import KafkaConsumer
from loading.mongodbloader import MongoDBLoader
from loading.sqldbloader import SQLloader
import os

if __name__ == "__main__":
    # Configuraciones de Kafka y MongoDB
    kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:29092')
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://adminUser:12345@localhost:27017/hr_data')

    # Inicializa el loader de MongoDB
    mongo_loader = MongoDBLoader(uri=mongo_uri)
    # Inicializa el loader de MongoDB
    sql_loader = SQLloader()

    # Inicializa el consumidor de Kafka
    kafka_consumer = KafkaConsumer(kafka_broker, "hrpro-group", mongo_loader, sql_loader)

    # Comienza a consumir mensajes de Kafka y cargarlos en las bases de datos
    kafka_consumer.start_consuming()