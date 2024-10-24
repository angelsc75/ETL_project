import redis
import json
from logger import logger

class RedisLoader:
    def __init__(self, host='localhost', port=6379, db=0, buffer_size=1000):
        """
        Inicializa la conexión con Redis.
        """
        try:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True
            )
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
        
        # Limpiar datos anteriores al iniciar
        self.redis_client.delete(self.processing_list)
        self.redis_client.set(self.buffer_count_key, 0)
        logger.info(f"Buffer Redis inicializado - Tamaño máximo: {buffer_size}")
        print(f"Buffer Redis inicializado - Tamaño máximo: {buffer_size}")

    def add_to_buffer(self, data):
        """
        Añade datos al buffer de Redis.
        """
        try:
            pipeline = self.redis_client.pipeline()
            json_data = json.dumps(data)
            
            # Añadir datos a la lista
            pipeline.rpush(self.processing_list, json_data)
            # Incrementar el contador
            pipeline.incr(self.buffer_count_key)
            pipeline.execute()
            
            # Obtener el tamaño actual del buffer
            buffer_size = int(self.redis_client.get(self.buffer_count_key) or 0)
            
            logger.info(f"📥 Dato añadido a Redis - Buffer actual: {buffer_size}/{self.buffer_size}")
            if buffer_size % 100 == 0:  # Mostrar cada 100 mensajes
                print(f"📥 Buffer Redis: {buffer_size}/{self.buffer_size}")
            
            return buffer_size >= self.buffer_size
            
        except Exception as e:
            logger.error(f"❌ Error al añadir datos a Redis: {e}")
            print(f"❌ Error al añadir datos a Redis: {e}")
            logger.error(f"Datos que causaron el error: {data}")
            raise e

    def get_buffer_batch(self):
        """
        Obtiene un lote de datos del buffer y los elimina.
        """
        try:
            pipeline = self.redis_client.pipeline()
            # Obtener todos los elementos de la lista
            pipeline.lrange(self.processing_list, 0, -1)
            # Eliminar todos los elementos de la lista
            pipeline.delete(self.processing_list)
            # Resetear el contador
            pipeline.set(self.buffer_count_key, 0)
            
            results = pipeline.execute()
            
            if results and results[0]:
                batch_data = [json.loads(item) for item in results[0]]
                logger.info(f"📤 Batch obtenido de Redis - {len(batch_data)} elementos")
                print(f"📤 Batch obtenido de Redis - {len(batch_data)} elementos")
                return batch_data
                
            logger.warning("⚠️ No se encontraron datos en el buffer de Redis")
            print("⚠️ No se encontraron datos en el buffer de Redis")
            return []
            
        except Exception as e:
            logger.error(f"❌ Error al obtener datos de Redis: {e}")
            print(f"❌ Error al obtener datos de Redis: {e}")
            raise e

    def close(self):
        """
        Cierra la conexión con Redis.
        """
        try:
            self.redis_client.close()
            logger.info("✅ Conexión a Redis cerrada correctamente")
            print("✅ Conexión a Redis cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error al cerrar la conexión con Redis: {e}")
            print(f"❌ Error al cerrar la conexión con Redis: {e}")