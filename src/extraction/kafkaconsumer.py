from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient

class KafkaConsumer:
    
    def __init__(self, bootstrap_servers, group_id):
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
        }
        # Crear el consumidor
        self.consumer = Consumer(self.conf)
        # Crear el cliente de administración por separado
        self.admin_conf = {
            'bootstrap.servers': bootstrap_servers
        }
        self.admin_client = AdminClient(self.admin_conf)
        

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
                    # Procesar el mensaje
                    print(f"Mensaje recibido: {msg.value().decode('utf-8')}")
        except KeyboardInterrupt:
            pass
        finally:
            # Cerrar el consumidor
            self.consumer.close()


# Inicializar y consumir mensajes
# kafka_consumer = KafkaConsumer("localhost:29092", "hrpro-group")
# kafka_consumer.start_consuming()
