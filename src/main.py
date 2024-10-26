from src.extraction.kafkaconsumer import KafkaConsumer
from src.loading.mongodbloader import MongoDBLoader
from src.loading.sqldbloader import SQLloader
from src.loading.redisloader import RedisLoader
import os
from dotenv import main

main.load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

if __name__ == "__main__":
    # Configuraciones Kafka
    kafka_broker = os.getenv('KAFKA_BROKER')
    kafka_group = os.getenv('KAFKA_GROUP_ID')
    
    # Configuración Redis
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    redis_db = int(os.getenv('REDIS_DB', 0))
    redis_buffer_size = int(os.getenv('REDIS_BUFFER_SIZE', 1000))
    
    # Configuración MongoDB - usando nombres específicos para MongoDB
    mongo_uri = os.getenv('MONGO_URI')
    mongo_db_name = os.getenv('MONGO_DB')  # Usando MONGO_DB específicamente para MongoDB
    mongo_collection = os.getenv('MONGO_COLLECTION')
    
    # Configuración PostgreSQL - usando nombres específicos para PostgreSQL
    postgres_host = os.getenv('DB_HOST')
    postgres_port = os.getenv('DB_PORT')
    postgres_db = os.getenv('DB_DB')  # Usando DB_DB específicamente para PostgreSQL
    postgres_schema = os.getenv('DB_SCHEMA')
    postgres_user = os.getenv('DB_USER')
    postgres_password = os.getenv('DB_PASSWORD')

    # Inicializar los loaders
    redis_loader = RedisLoader(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        buffer_size=redis_buffer_size
    )
    
    mongo_loader = MongoDBLoader(
        uri=mongo_uri,
        db_name=mongo_db_name,
        collection_name=mongo_collection
    )
    
    sql_loader = SQLloader(
        host=postgres_host,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )

    # Inicializar el consumidor de Kafka con todos los loaders
    kafka_consumer = KafkaConsumer(
        kafka_broker,
        kafka_group,
        redis_loader,
        mongo_loader,
        sql_loader
    )

    # Comenzar a consumir mensajes
    kafka_consumer.start_consuming()