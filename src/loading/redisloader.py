import redis
import json
from logger import logger

class RedisLoader:
    def __init__(self, host='localhost', port=6379, db=0, buffer_size=1000):
        """
        Inicializa la conexión con Redis con optimizaciones de rendimiento.
        """
        try:
            # Configuración del pool de conexiones
            self.redis_pool = redis.ConnectionPool(
                host=host,
                port=port,
                db=db,
                decode_responses=True,
                max_connections=10  # Ajustar según necesidades
            )
            self.redis_client = redis.Redis(connection_pool=self.redis_pool)
            # Verificar conexión
            self.redis_client.ping()
            logger.info(f"✅ Conexión exitosa a Redis en {host}:{port}")
            print(f"✅ Conexión exitosa a Redis en {host}:{port}")
        except Exception as e:
            logger.error(f"❌ Error al conectar con Redis: {e}")
            print(f"❌ Error al conectar con Redis: {e}")
            raise e
            
        self.buffer_size = buffer_size
        self.processing_list = "processing_queue"
        self.buffer_count_key = "buffer_count"
        
        # Usar pipelines para limpiar datos anteriores
        with self.redis_client.pipeline() as pipe:
            pipe.delete(self.processing_list)
            pipe.set(self.buffer_count_key, 0)
            pipe.execute()
            
        logger.info(f"Buffer Redis inicializado - Tamaño máximo: {buffer_size}")
        print(f"Buffer Redis inicializado - Tamaño máximo: {buffer_size}")

    def add_to_buffer(self, data):
        """
        Añade datos al buffer de Redis de manera optimizada usando pipeline
        y minimizando las operaciones de red.
        """
        try:
            with self.redis_client.pipeline() as pipe:
                # Preparar datos en formato JSON
                json_data = json.dumps(data)
                
                # Ejecutar operaciones en batch
                pipe.multi()
                pipe.rpush(self.processing_list, json_data)
                pipe.incr(self.buffer_count_key)
                pipe.get(self.buffer_count_key)
                results = pipe.execute()
                
                # El último resultado es el conteo actual
                buffer_size = int(results[-1])
                
                if buffer_size % 100 == 0:  # Mostrar cada 100 mensajes
                    logger.info(f"📥 Buffer Redis: {buffer_size}/{self.buffer_size}")
                    print(f"📥 Buffer Redis: {buffer_size}/{self.buffer_size}")
                
                return buffer_size >= self.buffer_size
                
        except redis.RedisError as e:
            logger.error(f"❌ Error de Redis al añadir datos: {e}")
            print(f"❌ Error de Redis al añadir datos: {e}")
            raise e
        except Exception as e:
            logger.error(f"❌ Error general al añadir datos: {e}")
            print(f"❌ Error general al añadir datos: {e}")
            logger.error(f"Datos que causaron el error: {data}")
            raise e

    def get_buffer_batch(self):
        """
        Obtiene un lote de datos del buffer de manera optimizada usando
        pipeline y minimizando las operaciones de lectura/escritura.
        """
        try:
            with self.redis_client.pipeline() as pipe:
                # Ejecutar todas las operaciones en una sola transacción
                pipe.multi()
                pipe.lrange(self.processing_list, 0, -1)
                pipe.delete(self.processing_list)
                pipe.set(self.buffer_count_key, 0)
                results = pipe.execute()
                
                batch_data = results[0]  # Resultado de lrange
                
                if batch_data:
                    # Procesar datos en batch para mejor rendimiento
                    processed_batch = [json.loads(item) for item in batch_data]
                    batch_size = len(processed_batch)
                    logger.info(f"📤 Batch obtenido de Redis - {batch_size} elementos")
                    print(f"📤 Batch obtenido de Redis - {batch_size} elementos")
                    return processed_batch
                
                logger.warning("⚠️ No se encontraron datos en el buffer de Redis")
                print("⚠️ No se encontraron datos en el buffer de Redis")
                return []
                
        except redis.RedisError as e:
            logger.error(f"❌ Error de Redis al obtener batch: {e}")
            print(f"❌ Error de Redis al obtener batch: {e}")
            raise e
        except Exception as e:
            logger.error(f"❌ Error general al obtener batch: {e}")
            print(f"❌ Error general al obtener batch: {e}")
            raise e

    def close(self):
        """
        Cierra la conexión con Redis de manera segura.
        """
        try:
            # Limpiar datos antes de cerrar
            with self.redis_client.pipeline() as pipe:
                pipe.delete(self.processing_list)
                pipe.delete(self.buffer_count_key)
                pipe.execute()
            
            # Cerrar el pool de conexiones
            self.redis_pool.disconnect()
            logger.info("✅ Conexión a Redis cerrada correctamente")
            print("✅ Conexión a Redis cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error al cerrar la conexión con Redis: {e}")
            print(f"❌ Error al cerrar la conexión con Redis: {e}")