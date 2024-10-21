from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
from transformation.datatransformer import process_and_group_data  # Importar la función de transformación
import json
from logger import logger

class KafkaConsumer:
    
    def __init__(self, bootstrap_servers, group_id, mongo_loader, sql_loader, batch_size=100):
        """
        Inicializa el consumidor de Kafka y el cliente de MongoDB.
        
        :param bootstrap_servers: Dirección y puerto del servidor Kafka.
        :param group_id: Grupo de consumidores de Kafka.
        :param mongo_loader: Instancia de MongoDBLoader para cargar los mensajes en MongoDB.
        :param batch_size: Tamaño del lote para guardado en MongoDB.
        """
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
        }
        # Crear el consumidor
        self.consumer = Consumer(self.conf)
        
        # Crear el cliente de administración de Kafka
        self.admin_conf = {
            'bootstrap.servers': bootstrap_servers,
        }
        self.admin_client = AdminClient(self.admin_conf)

        # Loader para MongoDB
        self.mongo_loader = mongo_loader  # Pasamos el loader de MongoDB desde main.py
        # Loader para MongoDB
        self.sql_loader = sql_loader  # Pasamos el loader de MongoDB desde main.py
        self.batch_size = batch_size  # Tamaño del lote para guardar
        self.message_buffer = []  # Buffer para acumular mensajes en memoria

    def start_consuming(self):
        # Paso 1: Obtener la lista de tópicos
        metadata = self.admin_client.list_topics(timeout=10)
        print("Tópicos disponibles:")
        logger.info("Tópicos disponibles:")
        
        for topic in metadata.topics:
            print(f"Tópico: {topic}")

        # Obtener el primer tópico encontrado
        if not metadata.topics:
            print("No hay tópicos disponibles.")
            logger.error("No hay tópicos disponibles.")
            return

        primer_topico = list(metadata.topics.keys())[0]

        # Suscribir el consumidor al primer tópico
        self.consumer.subscribe([primer_topico])
        print(f"Suscrito al tópico: {primer_topico}")
        logger.info(f"Suscrito al tópico: {primer_topico}")

        try:
            while True:
                msg = self.consumer.poll(1.0)  # Poll the topic
                if msg is None:
                    continue
                if msg.error():
                    # Manejar el error del mensaje
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Fin de la partición
                        print("Fin de la partición")
                        logger.error("Fin de la partición")
                    else:
                        print(f"Error: {msg.error()}")
                else:
                    # Procesar el mensaje recibido
                    raw_message  = msg.value().decode('utf-8')
                    # print(f"Mensaje recibido: {raw_message}")

                    # Transformar el mensaje usando el datatransformer
                    transformed_data = process_and_group_data(raw_message)
                    self.message_buffer.append(transformed_data)

                    # Guardar los datos transformados
                    if len(self.message_buffer) >= self.batch_size:
                        self.save_messages()

        except KeyboardInterrupt:
            logger.error("KeyboardInterrupt")
        finally:
            # Guardar los mensajes restantes en el buffer antes de cerrar
            if self.message_buffer:
                self.save_messages()
            # Cerrar el consumidor y la conexión a MongoDB
            self.consumer.close()
            self.mongo_loader.close()

    def save_messages_mongo(self, message_buffer):
        """
        Guarda el lote actual de mensajes en MongoDB y maneja los errores si falla.
        """
        try:
            print(f"Guardando {len(message_buffer)} mensajes en MongoDB...")
            self.mongo_loader.load_to_mongodb(message_buffer)
        except Exception as e:
            print(f"Error al guardar en MongoDB: {e}")
            logger.error(f"Error al guardar en MongoDB: {e}")
            # Guardar los mensajes fallidos en un archivo de log para procesarlos más tarde
            for message in message_buffer:
                logger.error(f"Mensaje fallido: {json.dumps(message)}\n")
        
    def save_messages_sql(self, message_buffer):
        """
        Guarda el lote actual de mensajes en Postgres y maneja los errores si falla.
        """
        try:
            print(f"Guardando {len(message_buffer)} mensajes en Postgres...")
            self.sql_loader.load_to_sql(message_buffer)
        except Exception as e:
            print(f"Error al guardar en Postgres: {e}")
            logger.error(f"Error al guardar en MongoDB: {e}")
            # Guardar los mensajes fallidos en un archivo de log para procesarlos más tarde
            for message in message_buffer:
                logger.error(f"Mensaje fallido: {json.dumps(message)}\n")
    
    def save_messages(self):
        """
        Guarda el lote actual de mensajes en MongoDB y Postgres y maneja los errores si falla.
        """
        try:
            self.save_messages_mongo(self.message_buffer)
            self.save_messages_sql(self.message_buffer)
        except Exception as e:
            print(f"\nException as: {e}\n")
            logger.error(f"Exception as: {e}\n")
        
        finally:
            # Limpiar el buffer después de guardar
            self.message_buffer = []

# Esta clase es llamada e inicializada desde main.py, no directamente desde aquí.
