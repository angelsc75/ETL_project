from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
from transformation.datatransformer import process_and_group_data
import json

class KafkaConsumer:
    def __init__(self, bootstrap_servers, group_id, redis_loader, mongo_loader, sql_loader, batch_size=100):
        """
        Inicializa el consumidor de Kafka y los loaders.
        """
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
        }
        try:
            self.consumer = Consumer(self.conf)
            self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
            logger.info(f"✅ Conexión exitosa a Kafka en {bootstrap_servers}")
            print(f"✅ Conexión exitosa a Kafka en {bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Error al conectar con Kafka: {e}")
            print(f"❌ Error al conectar con Kafka: {e}")
            raise e

        self.redis_loader = redis_loader
        self.mongo_loader = mongo_loader
        self.sql_loader = sql_loader
        self.batch_size = batch_size
        self.message_count = 0

    def start_consuming(self):
<<<<<<< Updated upstream
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

=======
>>>>>>> Stashed changes
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            available_topics = list(metadata.topics.keys())
            logger.info(f"📋 Tópicos disponibles: {available_topics}")
            print(f"📋 Tópicos disponibles: {available_topics}")
            
            if not available_topics:
                logger.error("❌ No hay tópicos disponibles")
                print("❌ No hay tópicos disponibles")
                return

            primer_topico = "probando"
            self.consumer.subscribe([primer_topico])
            logger.info(f"✅ Suscrito al tópico: {primer_topico}")
            print(f"✅ Suscrito al tópico: {primer_topico}")

            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
<<<<<<< Updated upstream
                        # Fin de la partición
                        print("Fin de la partición")
=======
                        logger.warning("⚠️ Fin de la partición")
                        print("⚠️ Fin de la partición")
>>>>>>> Stashed changes
                    else:
                        logger.error(f"❌ Error Kafka: {msg.error()}")
                        print(f"❌ Error Kafka: {msg.error()}")
                else:
                    self.message_count += 1
                    raw_message = msg.value().decode('utf-8')
                    
                    if self.message_count % 100 == 0:  # Mostrar cada 100 mensajes
                        logger.info(f"📨 Mensajes procesados: {self.message_count}")
                        print(f"📨 Mensajes procesados: {self.message_count}")
                    
                    try:
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
<<<<<<< Updated upstream
            pass
=======
            logger.info("👋 Deteniendo el consumidor por interrupción del usuario")
            print("👋 Deteniendo el consumidor por interrupción del usuario")
        except Exception as e:
            logger.error(f"❌ Error general en el consumidor: {e}")
            print(f"❌ Error general en el consumidor: {e}")
>>>>>>> Stashed changes
        finally:
            logger.info("🔄 Procesando mensajes finales...")
            print("🔄 Procesando mensajes finales...")
            self.process_batch()
            self.consumer.close()
            self.redis_loader.close()
            self.mongo_loader.close()

    def process_batch(self):
        """
        Procesa un lote de datos desde Redis y los guarda en MongoDB y PostgreSQL.
        """
        try:
            batch_data = self.redis_loader.get_buffer_batch()
            
            if batch_data:
                logger.info(f"📦 Procesando batch de {len(batch_data)} mensajes")
                print(f"📦 Procesando batch de {len(batch_data)} mensajes")
                
                # Guardar en MongoDB
                self.save_messages_mongo(batch_data)
                # Guardar en PostgreSQL
                self.save_messages_sql(batch_data)
                
                logger.info("✅ Batch procesado exitosamente")
                print("✅ Batch procesado exitosamente")
            else:
                logger.warning("⚠️ No hay datos para procesar en el batch")
                print("⚠️ No hay datos para procesar en el batch")
                
        except Exception as e:
<<<<<<< Updated upstream
            print(f"Error al guardar en MongoDB: {e}")
            # Guardar los mensajes fallidos en un archivo de log para procesarlos más tarde
            with open('failed_messages.log', 'a') as log_file:
                for message in message_buffer:
                    log_file.write(json.dumps(message) + '\n')
=======
            logger.error(f"❌ Error al procesar el batch: {e}")
            print(f"❌ Error al procesar el batch: {e}")

    def save_messages_mongo(self, message_buffer):
        try:
            if not message_buffer:
                logger.warning("⚠️ Buffer vacío - No hay mensajes para guardar en MongoDB")
                return
                
            logger.info(f"💾 Guardando {len(message_buffer)} mensajes en MongoDB...")
            print(f"💾 Guardando {len(message_buffer)} mensajes en MongoDB...")
            
            inserted_count = self.mongo_loader.load_to_mongodb(message_buffer)
            
            if inserted_count:
                logger.info(f"✅ {inserted_count} mensajes guardados en MongoDB")
                print(f"✅ {inserted_count} mensajes guardados en MongoDB")
                    
        except Exception as e:
            logger.error(f"❌ Error al guardar en MongoDB: {e}")
            print(f"❌ Error al guardar en MongoDB: {e}")
>>>>>>> Stashed changes
        
    def save_messages_sql(self, message_buffer):
        try:
            logger.info(f"💾 Guardando {len(message_buffer)} mensajes en PostgreSQL...")
            print(f"💾 Guardando {len(message_buffer)} mensajes en PostgreSQL...")
            self.sql_loader.load_to_sql(message_buffer)
            logger.info("✅ Datos guardados exitosamente en PostgreSQL")
            print("✅ Datos guardados exitosamente en PostgreSQL")
        except Exception as e:
<<<<<<< Updated upstream
            print(f"Error al guardar en MongoDB: {e}")
            # Guardar los mensajes fallidos en un archivo de log para procesarlos más tarde
            with open('failed_messages.log', 'a') as log_file:
                for message in message_buffer:
                    log_file.write(json.dumps(message) + '\n')
    
    def save_messages(self):
        """
        Guarda el lote actual de mensajes en MongoDB y Postgres y maneja los errores si falla.
        """
        try:
            self.save_messages_mongo(self.message_buffer)
            self.save_messages_sql(self.message_buffer)
        except Exception as e:
            print(f"Exception as {e}:")
        
        finally:
            # Limpiar el buffer después de guardar
            self.message_buffer = []

# Esta clase es llamada e inicializada desde main.py, no directamente desde aquí.
=======
            logger.error(f"❌ Error al guardar en PostgreSQL: {e}")
            print(f"❌ Error al guardar en PostgreSQL: {e}")
            for message in message_buffer:
                # Crear una copia del mensaje y eliminar campos no serializables
                safe_message = message.copy()
                if '_id' in safe_message:
                    safe_message['_id'] = str(safe_message['_id'])  # Convertir ObjectId a string
                logger.error(f"Mensaje fallido: {json.dumps(safe_message)}")
>>>>>>> Stashed changes
