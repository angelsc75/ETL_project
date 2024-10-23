from extraction.kafkaconsumer import KafkaConsumer
from loading.mongodbloader import MongoDBLoader
from loading.sqldbloader import SQLloader
import os
from dotenv import main

main.load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))







if __name__ == "__main__":
    # Configuraciones 
    kafka_broker = os.getenv('KAFKA_BROKER')
    mongo_uri = os.getenv('MONGO_URI')
    db_name= os.getenv('DB_NAME')
    collection_name= os.getenv('COLLECTION_NAME')


    db_type = os.getenv('DB_TYPE')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_DB')
    db_schema = os.getenv('DB_SCHEMA')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASSWORD')

    # Inicializa el loader de MongoDB
    mongo_loader = MongoDBLoader(uri=mongo_uri, db_name=db_name, collection_name=collection_name)
    # Inicializa el loader de MongoDB
    sql_loader = SQLloader(host=db_host, database=db_name, 
    user=db_user, password=db_pass, port=db_port)

    # Inicializa el consumidor de Kafka
    kafka_consumer = KafkaConsumer(kafka_broker, "hrpro-group", mongo_loader, sql_loader)

    # Comienza a consumir mensajes de Kafka y cargarlos en las bases de datos
    kafka_consumer.start_consuming()