# Importaciones necesarias
from src.extraction.kafkaconsumer import KafkaConsumer  # Maneja la conexión y consumo de mensajes de Kafka
from src.loading.mongodbloader import MongoDBLoader    # Maneja la carga de datos en MongoDB
from src.loading.sqldbloader import SQLloader         # Maneja la carga de datos en PostgreSQL
from src.loading.redisloader import RedisLoader       # Maneja el buffer temporal en Redis
import os                                            # Para manejar variables de entorno y paths
from dotenv import main                              # Para cargar variables desde archivo .env

# Carga las variables de entorno desde el archivo .env
# Busca el archivo .env un nivel arriba del directorio actual
main.load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

if __name__ == "__main__":
    # CONFIGURACIÓN DE KAFKA
    # Obtiene la dirección del broker y el ID del grupo de consumidores
    kafka_broker = "kafka:9092"         # Dirección del servidor Kafka
    kafka_group = os.getenv('KAFKA_GROUP_ID')        # ID del grupo de consumidores
    
    # CONFIGURACIÓN DE REDIS
    # Configuración para el buffer temporal que almacena mensajes antes de procesarlos en lote
    redis_host = os.getenv('REDIS_HOST', 'localhost')  # Host de Redis (default: localhost)
    redis_port = int(os.getenv('REDIS_PORT', 6379))   # Puerto de Redis (default: 6379)
    redis_db = int(os.getenv('REDIS_DB', 0))          # Número de base de datos Redis (default: 0)
    redis_buffer_size = int(os.getenv('REDIS_BUFFER_SIZE', 1000))  # Tamaño del buffer antes de procesar (default: 1000)
    
    # CONFIGURACIÓN DE MONGODB
    # Parámetros de conexión específicos para MongoDB
    mongo_uri = os.getenv('MONGO_URI')              # URI de conexión a MongoDB
    mongo_db_name = os.getenv('MONGO_DB')           # Nombre de la base de datos MongoDB
    mongo_collection = os.getenv('MONGO_COLLECTION') # Nombre de la colección en MongoDB
    
    # CONFIGURACIÓN DE POSTGRESQL
    # Parámetros de conexión específicos para PostgreSQL
    postgres_host = os.getenv('DB_HOST')           # Host de PostgreSQL
    postgres_port = os.getenv('DB_PORT')           # Puerto de PostgreSQL
    postgres_db = os.getenv('DB_DB')               # Nombre de la base de datos PostgreSQL
    postgres_schema = os.getenv('DB_SCHEMA')       # Schema de PostgreSQL
    postgres_user = os.getenv('DB_USER')           # Usuario de PostgreSQL
    postgres_password = os.getenv('DB_PASSWORD')    # Contraseña de PostgreSQL

    # INICIALIZACIÓN DE LOADERS
    # 1. Inicializar Redis Loader (Buffer temporal)
    redis_loader = RedisLoader(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        buffer_size=redis_buffer_size
    )
    
    # 2. Inicializar MongoDB Loader (Almacenamiento NoSQL)
    mongo_loader = MongoDBLoader(
        uri=mongo_uri,
        db_name=mongo_db_name,
        collection_name=mongo_collection
    )
    
    # 3. Inicializar SQL Loader (Almacenamiento relacional)
    sql_loader = SQLloader(
        host=postgres_host,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )

    # INICIALIZACIÓN DEL CONSUMIDOR KAFKA
    # Crea una instancia del consumidor con todos los loaders configurados
    kafka_consumer = KafkaConsumer(
        kafka_broker,
        kafka_group,
        redis_loader,
        mongo_loader,
        sql_loader
    )

    # INICIO DEL PROCESO DE CONSUMO
    # Comienza a consumir mensajes de Kafka de manera continua
    kafka_consumer.start_consuming()