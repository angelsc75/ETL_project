from extraction.kafkaconsumer import KafkaConsumer
# from transformation.datatransformer import DataTransformer
# from loading.mongodataloader import MongoDBLoader
# from loading.sql_loader import PostgreSQLLoader
import os

# def process_message(data):
#     # Inicializa el transformador
#     transformer = DataTransformer()
#     processed_data = transformer.transform_message(data)
    
#     # Guarda en MongoDB
#     mongo_loader.save_data(processed_data)
    
#     # Guarda en PostgreSQL
#     postgres_loader.save_data(processed_data)

if __name__ == "__main__":
    # Configuraciones de conexión desde variables de entorno
    kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
    # mongo_uri = os.getenv('MONGO_URI', 'mongodb://root:example@localhost:27017')
    # postgres_uri = os.getenv('POSTGRES_URI', 'postgresql://user:password@localhost:5432/mydatabase')

    # Inicializa el consumidor de Kafka
    kafka_consumer = KafkaConsumer("localhost:29092", "hrpro-group")

    # # Inicializa los loaders para MongoDB y PostgreSQL
    # mongo_loader = MongoDBLoader(mongo_uri, 'mydatabase', 'kafka_data')
    # postgres_loader = PostgreSQLLoader(postgres_uri)

    
    kafka_consumer.start_consuming()

        # mongo_loader.close()
        # postgres_loader.close()
