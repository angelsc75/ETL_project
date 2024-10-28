from confluent_kafka import Consumer, Producer  # Cliente Kafka para consumo de mensajes
from confluent_kafka.admin import  AdminClient        # Cliente admin para gestionar tópicos
from src.transformation.datatransformer import process_and_group_data  # Procesamiento de datos
import json
from src.logger import logger                       # Logger personalizado para el sistema
import os


class KafkaConsumer:
    def __init__(self, bootstrap_servers, group_id, redis_loader, mongo_loader, sql_loader):
        """
        Constructor del consumidor Kafka que inicializa las conexiones y loaders.
        
        Args:
            bootstrap_servers (str): Dirección del servidor Kafka
            group_id (str): ID del grupo de consumidores
            redis_loader (RedisLoader): Gestor del buffer temporal
            mongo_loader (MongoDBLoader): Gestor de almacenamiento en MongoDB
            sql_loader (SQLloader): Gestor de almacenamiento en PostgreSQL
        """
        # Configuración básica del consumidor Kafka
        kafka_host = os.getenv('KAFKA_HOST', 'kafka')  # Por defecto usar 'kafka' como hostname
        kafka_port = os.getenv('KAFKA_PORT', '29092')
        self.bootstrap_servers = f"{kafka_host}:{kafka_port}"

        self.config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'mi-grupo',
            'auto.offset.reset': 'earliest'
        }

        self.consumer = Consumer(self.config)
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})
        self.admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        try:
            # Inicializar el consumidor y el cliente admin
            self.consumer = Consumer(self.config)
            self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
            logger.info(f"✅ Conexión exitosa a Kafka en {bootstrap_servers}")
            print(f"✅ Conexión exitosa a Kafka en {bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Error al conectar con Kafka: {e}")
            print(f"❌ Error al conectar con Kafka: {e}")
            raise e

        # Almacenar referencias a los loaders
        self.redis_loader = redis_loader      # Buffer temporal
        self.mongo_loader = mongo_loader      # Almacenamiento NoSQL
        self.sql_loader = sql_loader          # Almacenamiento SQL
        self.message_count = 0                # Contador de mensajes procesados

    def start_consuming(self):
        """
        Inicia el proceso de consumo continuo de mensajes.
        Este método es el punto principal de procesamiento.
        """
        try:
            # Obtener lista de tópicos disponibles
            metadata = self.admin_client.list_topics(timeout=10)
            available_topics = list(metadata.topics.keys())
            logger.info(f"📋 Tópicos disponibles: {available_topics}")
            print(f"📋 Tópicos disponibles: {available_topics}")
            
            # Verificar si hay tópicos disponibles
            if not available_topics:
                logger.error("❌ No hay tópicos disponibles")
                print("❌ No hay tópicos disponibles")
                return

            # Suscribirse al primer tópico disponible
            primer_topico = available_topics[0]
            self.consumer.subscribe([primer_topico])
            logger.info(f"✅ Suscrito al tópico: {primer_topico}")
            print(f"✅ Suscrito al tópico: {primer_topico}")

            # Bucle principal de consumo
            while True:
                # Intentar obtener un mensaje (timeout 1 segundo)
                msg = self.consumer.poll(1.0)
                
                # Si no hay mensaje, continuar al siguiente ciclo
                if msg is None:
                    continue
                    
                # Manejar errores de Kafka
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.warning("⚠️ Fin de la partición")
                        print("⚠️ Fin de la partición")
                    else:
                        logger.error(f"❌ Error Kafka: {msg.error()}")
                        print(f"❌ Error Kafka: {msg.error()}")
                    continue

                # Incrementar contador de mensajes y logging periódico
                self.message_count += 1
                if self.message_count % 1000 == 0:
                    logger.info(f"📨 Mensajes procesados: {self.message_count}")
                    print(f"📨 Mensajes procesados: {self.message_count}")

                try:
                    # Procesar el mensaje recibido
                    raw_message = msg.value().decode('utf-8')  # Decodificar mensaje
                    transformed_data = process_and_group_data(raw_message)  # Transformar datos

                    # Si no hay error en la transformación
                    if "error" not in transformed_data:
                        # Añadir al buffer de Redis
                        buffer_full = self.redis_loader.add_to_buffer(transformed_data)
                        
                        # Si el buffer está lleno, procesar el lote
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

    def process_batch(self):
        """
        Procesa un lote completo de mensajes desde el buffer Redis.
        Los datos se guardan en MongoDB y PostgreSQL.
        """
        try:
            # Obtener datos del buffer
            batch_data = self.redis_loader.get_buffer_batch()
            
            # Verificar si hay datos para procesar
            if not batch_data:
                logger.warning("⚠️ No hay datos para procesar en el batch")
                print("⚠️ No hay datos para procesar en el batch")
                return

            logger.info(f"📦 Procesando batch de {len(batch_data)} mensajes")
            print(f"📦 Procesando batch de {len(batch_data)} mensajes")
            
            # Guardar datos en MongoDB
            self.mongo_loader.load_to_mongodb(batch_data)
            logger.info(f"✅ Datos guardados en MongoDB")
            
            # Guardar datos en PostgreSQL
            self.sql_loader.load_to_sql(batch_data)
            logger.info(f"✅ Datos guardados en PostgreSQL")
            
            logger.info("✅ Batch procesado exitosamente")
            print("✅ Batch procesado exitosamente")
            
        except Exception as e:
            logger.error(f"❌ Error al procesar el batch: {e}")
            print(f"❌ Error al procesar el batch: {e}")
            raise e

    def cleanup(self):
        """
        Realiza la limpieza final antes de cerrar el consumidor.
        Procesa mensajes pendientes y cierra conexiones.
        """
        try:
            logger.info("🧹 Iniciando limpieza...")
            print("🧹 Iniciando limpieza...")
            
            # Procesar mensajes finales pendientes
            logger.info("🔄 Procesando mensajes finales...")
            print("🔄 Procesando mensajes finales...")
            self.process_batch()
            
            # Cerrar todas las conexiones
            self.consumer.close()
            self.redis_loader.close()
            self.mongo_loader.close()
            
            logger.info("✅ Limpieza completada exitosamente")
            print("✅ Limpieza completada exitosamente")
        except Exception as e:
            logger.error(f"❌ Error durante la limpieza: {e}")
            print(f"❌ Error durante la limpieza: {e}")