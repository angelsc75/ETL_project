import json
import logging
from confluent_kafka import Consumer, KafkaError
import redis
from pymongo import MongoClient
import threading
import time

from etl3 import procesar_datos

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuraciones
kafka_config = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}
kafka_topic = 'probando'
redis_list = 'kafka_messages'
batch_size = 100

# Conexiones
consumer = None
mongo_client = None
mongo_db = None
mongo_collection = None
redis_client = None

def initialize_connections():
    global consumer, mongo_client, mongo_db, mongo_collection, redis_client
    try:
        consumer = Consumer(kafka_config)
        consumer.subscribe([kafka_topic])
        logging.info(f"Subscrito al topic de Kafka: {kafka_topic}")
    except Exception as e:
        logging.error(f"Error al conectar con Kafka: {e}")
        consumer = None

    try:
        mongo_client = MongoClient('mongodb://localhost:27017/')
        mongo_db = mongo_client['hr_data']
        mongo_collection = mongo_db['hr_data']
        logging.info("Conexión exitosa a MongoDB")
    except Exception as e:
        logging.error(f"Error al conectar con MongoDB: {e}")
        mongo_client = None

    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        redis_client.ping()
        logging.info("Conexión exitosa a Redis")
    except redis.ConnectionError as e:
        logging.error(f"Error de conexión inicial a Redis: {e}")
        redis_client = None

def kafka_to_redis():
    global consumer, redis_client
    if consumer is None or redis_client is None:
        logging.error("No se puede iniciar kafka_to_redis debido a errores de conexión")
        return

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info('Reached end of partition')
                else:
                    logging.error(f'Error: {msg.error()}')
            else:
                value = msg.value().decode('utf-8')
                redis_client.rpush(redis_list, value)
                logging.info(f"Mensaje recibido de Kafka y almacenado en Redis: {value[:50]}...")
    except Exception as e:
        logging.error(f"Error en kafka_to_redis: {e}")
    finally:
        consumer.close()

def redis_to_mongodb():
    global redis_client, mongo_collection
    if redis_client is None or mongo_collection is None:
        logging.error("No se puede iniciar redis_to_mongodb debido a errores de conexión")
        return

    while True:
        try:
            batch = []
            for _ in range(batch_size):
                message = redis_client.lpop(redis_list)
                if message is None:
                    break
                batch.append(json.loads(message))
            
            if batch:
                mongo_collection.insert_many(batch)
                logging.info(f"Batch de {len(batch)} mensajes insertados en MongoDB")
            else:
                time.sleep(1)
        except redis.ConnectionError as e:
            logging.error(f"Error de conexión a Redis: {e}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error en redis_to_mongodb: {e}")
            time.sleep(1)

if __name__ == "__main__":
    logging.info("Iniciando pipeline Kafka -> Redis -> MongoDB")
    initialize_connections()
    
    if consumer is None or redis_client is None or mongo_client is None:
        logging.error("No se pueden iniciar los threads debido a errores de conexión")
    else:
        kafka_thread = threading.Thread(target=kafka_to_redis)
        mongodb_thread = threading.Thread(target=redis_to_mongodb)
        etl_tread=threading.Thread(target=procesar_datos)
        kafka_thread.start()
        mongodb_thread.start()
        etl_tread.start()

        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Deteniendo el pipeline...")
        finally:
            if consumer:
                consumer.close()
            if mongo_client:
                mongo_client.close()
            if redis_client:
                redis_client.close()
            logging.info("Pipeline detenido")