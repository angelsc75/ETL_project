import redis
import json
import zlib
import time
from datetime import datetime
from logger import logger

class RedisLoader:
    def __init__(self, host='localhost', port=6379, db=0, buffer_size=5000):
        """
        Inicializa la conexión con Redis con buffer optimizado.
        
        Args:
            host (str): Host de Redis
            port (int): Puerto de Redis
            db (int): Número de base de datos
            buffer_size (int): Tamaño máximo del buffer (default: 5000)
        """
        # Validación explícita del tamaño del buffer
        self.buffer_size = max(1000, buffer_size)  # Asegura mínimo 1000
        # self.buffer_size = buffer_size if buffer_size > 0 else 5000
        logger.info(f"🔧 Configurando buffer con tamaño: {self.buffer_size}")
        print(f"🔧 Configurando buffer con tamaño: {self.buffer_size}")
        
        # Configuración avanzada de Redis
        self.redis_config = {
            'host': host,
            'port': port,
            'db': db,
            'decode_responses': False,  # Necesario para compresión
            'socket_timeout': 5,
            'socket_connect_timeout': 5,
            'socket_keepalive': True,
            'health_check_interval': 30
        }
        
        try:
            # Configurar pool de conexiones
            self.redis_pool = redis.ConnectionPool(
                **self.redis_config,
                max_connections=20,
                retry_on_timeout=True
            )
            self.redis_client = redis.Redis(connection_pool=self.redis_pool)
            
            # Configurar política de memoria LRU
            self._configure_memory_policy()
            
            # Verificar conexión
            self.redis_client.ping()
            logger.info(f"✅ Conexión exitosa a Redis en {host}:{port}")
            print(f"✅ Conexión exitosa a Redis en {host}:{port}")
        except Exception as e:
            logger.error(f"❌ Error al conectar con Redis: {e}")
            print(f"❌ Error al conectar con Redis: {e}")
            raise e

        # Claves de Redis
        self.processing_list = "processing_queue"
        self.buffer_count_key = "buffer_count"
        self.last_flush_key = "last_flush_timestamp"
        self.buffer_stats_key = "buffer_stats"
        
        # Configuración adicional
        self.compression_enabled = True
        self.max_flush_interval = 60  # segundos
        self.monitor_interval = 100   # mostrar stats cada N mensajes
        
        # Inicialización
        self._initialize_redis()
        
    def _configure_memory_policy(self):
        """Configura la política de memoria LRU."""
        try:
            self.redis_client.config_set('maxmemory-policy', 'allkeys-lru')
            self.redis_client.config_set('maxmemory', '2gb')
        except Exception as e:
            logger.warning(f"⚠️ No se pudo configurar la política de memoria: {e}")

    def _initialize_redis(self):
        """Inicializa las estructuras de Redis y el buffer."""
        try:
            with self.redis_client.pipeline() as pipe:
                pipe.delete(self.processing_list)
                pipe.set(self.buffer_count_key, 0)
                pipe.set(self.last_flush_key, str(time.time()))
                pipe.delete(self.buffer_stats_key)
                pipe.execute()
            
            logger.info(f"✅ Buffer Redis inicializado - Tamaño configurado: {self.buffer_size}")
            print(f"✅ Buffer Redis inicializado - Tamaño configurado: {self.buffer_size}")
            self._update_buffer_stats(0, "inicialización")
        except Exception as e:
            logger.error(f"❌ Error al inicializar Redis: {e}")
            raise e

    def _update_buffer_stats(self, current_size, operation):
        """Actualiza y muestra estadísticas del buffer."""
        try:
            stats = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'current_size': current_size,
                'max_size': self.buffer_size,
                'percentage_full': round((current_size / self.buffer_size) * 100, 2),
                'operation': operation
            }
            
            self.redis_client.set(self.buffer_stats_key, json.dumps(stats))
            
            if current_size % self.monitor_interval == 0 or current_size >= self.buffer_size:
                logger.info(f"📊 Estado del buffer - "
                          f"Actual: {current_size}/{self.buffer_size} "
                          f"({stats['percentage_full']}%) - {operation}")
                print(f"📊 Estado del buffer - "
                     f"Actual: {current_size}/{self.buffer_size} "
                     f"({stats['percentage_full']}%) - {operation}")
        except Exception as e:
            logger.error(f"❌ Error al actualizar estadísticas: {e}")

    def _compress_data(self, data):
        """Comprime los datos usando zlib."""
        try:
            json_str = json.dumps(data)
            return zlib.compress(json_str.encode('utf-8'))
        except Exception as e:
            logger.warning(f"⚠️ Error en compresión, guardando sin comprimir: {e}")
            return json.dumps(data).encode('utf-8')

    def _decompress_data(self, compressed_data):
        """Descomprime los datos usando zlib."""
        try:
            decompressed = zlib.decompress(compressed_data)
            return json.loads(decompressed.decode('utf-8'))
        except zlib.error:
            return json.loads(compressed_data.decode('utf-8'))

    def _should_flush(self, current_size):
        """Determina si se debe realizar un flush basado en tiempo y tamaño."""
        try:
            current_time = time.time()
            last_flush = float(self.redis_client.get(self.last_flush_key) or 0)
            
            # Verificar condiciones de flush
            size_threshold_met = current_size >= self.buffer_size
            time_threshold_met = current_time - last_flush >= self.max_flush_interval
            
            if size_threshold_met:
                logger.info(f"🔄 Flush trigger: buffer lleno ({current_size}/{self.buffer_size})")
            elif time_threshold_met:
                logger.info(f"🔄 Flush trigger: tiempo máximo excedido ({int(current_time - last_flush)}s)")
            
            return size_threshold_met or time_threshold_met
        except Exception as e:
            logger.error(f"❌ Error al verificar condiciones de flush: {e}")
            return False

    def add_to_buffer(self, data):
        """
        Añade datos al buffer con monitoreo mejorado.
        """
        try:
            processed_data = self._compress_data(data) if self.compression_enabled else json.dumps(data).encode('utf-8')
            
            with self.redis_client.pipeline() as pipe:
                # Ejecutar operaciones en batch
                pipe.rpush(self.processing_list, processed_data)
                pipe.incr(self.buffer_count_key)
                pipe.get(self.buffer_count_key)
                results = pipe.execute()
                
                current_size = int(results[-1])
                self._update_buffer_stats(current_size, "añadido")
                
                return self._should_flush(current_size)
                
        except Exception as e:
            logger.error(f"❌ Error al añadir datos a Redis: {e}")
            print(f"❌ Error al añadir datos a Redis: {e}")
            raise e

    def get_buffer_batch(self):
        """
        Obtiene y procesa un lote de datos con monitoreo mejorado.
        """
        try:
            with self.redis_client.pipeline() as pipe:
                current_time = str(time.time())
                
                # Obtener tamaño actual antes de limpiar
                current_size = int(self.redis_client.get(self.buffer_count_key) or 0)
                logger.info(f"📤 Procesando batch de {current_size} elementos")
                print(f"📤 Procesando batch de {current_size} elementos")
                
                pipe.multi()
                pipe.lrange(self.processing_list, 0, -1)
                pipe.delete(self.processing_list)
                pipe.set(self.buffer_count_key, 0)
                pipe.set(self.last_flush_key, current_time)
                results = pipe.execute()
                
                compressed_batch = results[0]
                
                if compressed_batch:
                    batch_data = [self._decompress_data(item) for item in compressed_batch]
                    self._update_buffer_stats(0, "batch procesado")
                    return batch_data
                
                logger.warning("⚠️ No se encontraron datos en el buffer")
                print("⚠️ No se encontraron datos en el buffer")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error al obtener batch de Redis: {e}")
            print(f"❌ Error al obtener batch de Redis: {e}")
            raise e

    def close(self):
        """Cierra las conexiones y limpia recursos."""
        try:
            current_size = int(self.redis_client.get(self.buffer_count_key) or 0)
            if current_size > 0:
                logger.warning(f"⚠️ Cerrando con {current_size} mensajes en buffer")
                print(f"⚠️ Cerrando con {current_size} mensajes en buffer")
                pending_data = self.get_buffer_batch()
                if pending_data:
                    logger.info(f"✅ Procesados {len(pending_data)} mensajes pendientes")
                    print(f"✅ Procesados {len(pending_data)} mensajes pendientes")
            
            with self.redis_client.pipeline() as pipe:
                pipe.delete(self.processing_list)
                pipe.delete(self.buffer_count_key)
                pipe.delete(self.last_flush_key)
                pipe.delete(self.buffer_stats_key)
                pipe.execute()
            
            self.redis_pool.disconnect()
            logger.info("✅ Conexión a Redis cerrada correctamente")
            print("✅ Conexión a Redis cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error al cerrar la conexión con Redis: {e}")
            print(f"❌ Error al cerrar la conexión con Redis: {e}")