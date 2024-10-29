from confluent_kafka import Consumer, KafkaError     # Cliente Kafka para consumo de mensajes
from confluent_kafka.admin import AdminClient        # Cliente admin para gestionar tópicos
from transformation.datatransformer import process_and_group_data  # Procesamiento de datos
import json
from logger import logger                       # Logger personalizado para el sistema


class KafkaConsumer:
    def __init__(self, bootstrap_servers, group_id, redis_loader, mongo_loader, sql_loader):
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'socket.timeout.ms': 10000,
            'session.timeout.ms': 60000,
            'heartbeat.interval.ms': 20000,
            'max.poll.interval.ms': 300000,
            'debug': 'all'
        }
    
        logger.info(f"🔧 Intentando conectar a Kafka con configuración: {self.conf}")
    
        try:
            self.consumer = Consumer(self.conf)
            self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
            
            # Almacenar referencias a los loaders
            self.redis_loader = redis_loader
            self.mongo_loader = mongo_loader
            self.sql_loader = sql_loader
            self.message_count = 0
            
            # Debug metadata
            metadata = self.consumer.list_topics(timeout=10)
            logger.info(f"📊 Metadata del broker: {metadata}")
            logger.info(f"✅ Conexión exitosa a Kafka en {bootstrap_servers}")
            
        except Exception as e:
            logger.error(f"❌ Error al conectar con Kafka: {str(e)}")
            raise e

    def start_consuming(self):
        try:
            # Suscribirse al tópico
            topic = "probando"
            self.consumer.subscribe([topic])
            logger.info(f"✅ Suscrito al tópico: {topic}")
            print(f"✅ Suscrito al tópico: {topic}")

            # Bucle principal de consumo
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.warning("⚠️ Fin de la partición")
                        print("⚠️ Fin de la partición")
                    else:
                        logger.error(f"❌ Error Kafka: {msg.error()}")
                        print(f"❌ Error Kafka: {msg.error()}")
                    continue

                # Procesar mensaje
                self.message_count += 1
                if self.message_count % 1000 == 0:
                    logger.info(f"📨 Mensajes procesados: {self.message_count}")
                    print(f"📨 Mensajes procesados: {self.message_count}")

                try:
                    raw_message = msg.value().decode('utf-8')
                    transformed_data = process_and_group_data(raw_message)

                    if "error" not in transformed_data:
                        buffer_full = self.redis_loader.add_to_buffer(transformed_data)
                        if buffer_full:
                            logger.info("🔄 Buffer lleno - Iniciando procesamiento del batch")
                            print("🔄 Buffer lleno - Iniciando procesamiento del batch")
                            self.process_batch()
                    else:
                        logger.warning(f"⚠️ Mensaje inválido: {transformed_data['error']}")
                        print(f"⚠️ Mensaje inválido: {transformed_data['error']}")

                except Exception as e:
                    logger.error(f"❌ Error al procesar mensaje: {e}")
                    print(f"❌ Error al procesar mensaje: {e}")
                    logger.error(f"Mensaje que causó el error: {raw_message}")

        except KeyboardInterrupt:
            logger.info("👋 Deteniendo el consumidor por interrupción del usuario")
            print("👋 Deteniendo el consumidor por interrupción del usuario")
        finally:
            self.cleanup()