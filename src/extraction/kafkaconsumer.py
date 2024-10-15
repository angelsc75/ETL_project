from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
from loading.mongodbloader import MongoDBLoader
from transformation.datatransformer import process_and_group_data  # Importar la función de transformación

class KafkaConsumer:
    
    def __init__(self, bootstrap_servers, group_id, mongo_loader):
        """
        Inicializa el consumidor de Kafka y el cliente de MongoDB.
        
        :param bootstrap_servers: Dirección y puerto del servidor Kafka.
        :param group_id: Grupo de consumidores de Kafka.
        :param mongo_loader: Instancia de MongoDBLoader para cargar los mensajes en MongoDB.
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
            'bootstrap.servers': bootstrap_servers
        }
        self.admin_client = AdminClient(self.admin_conf)

        # Loader para MongoDB
        self.mongo_loader = mongo_loader  # Pasamos el loader de MongoDB desde main.py

    def start_consuming(self):
        # Paso 1: Obtener la lista de tópicos
        metadata = self.admin_client.list_topics(timeout=10)
        print("Tópicos disponibles:")
        
        for topic in metadata.topics:
            print(f"Tópico: {topic}")

        # Obtener el primer tópico encontrado
        if not metadata.topics:
            print("No hay tópicos disponibles.")
            return

        primer_topico = list(metadata.topics.keys())[0]

        # Suscribir el consumidor al primer tópico
        self.consumer.subscribe([primer_topico])
        print(f"Suscrito al tópico: {primer_topico}")

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
                    else:
                        print(f"Error: {msg.error()}")
                else:
                    # Procesar el mensaje recibido
                    raw_message  = msg.value().decode('utf-8')
                    print(f"Mensaje recibido: {raw_message}")

                    # Transformar el mensaje usando el datatransformer
                    transformed_data = process_and_group_data(raw_message)

                    # Guardar los datos transformados en MongoDB
                    self.mongo_loader.load_to_mongodb(transformed_data)

                    # Guardar los datos transformados en SQL
                    # self.sql_loader.insert_data(transformed_data)

        except KeyboardInterrupt:
            pass
        finally:
            # Cerrar el consumidor y la conexión a MongoDB
            self.consumer.close()
            self.mongo_loader.close()


# Esta clase es llamada e inicializada desde main.py, no directamente desde aquí.
