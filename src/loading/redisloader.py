import redis
from typing import List, Dict, Any
from logger import logger

class RedisLoader:
    """
    Clase para gestionar la carga y almacenamiento temporal de datos en Redis.
    Redis se utiliza como un buffer temporal antes de la persistencia final de los datos.
    """

    def __init__(self, host: str, port: int, db: int, buffer_size: int):
        """
        Constructor que inicializa la conexión con Redis y configura el sistema de buffer.
        
        Args:
            host (str): Dirección IP o hostname del servidor Redis
            port (int): Puerto en el que Redis está escuchando
            db (int): Número de la base de datos Redis (Redis permite múltiples DBs numeradas)
            buffer_size (int): Cantidad máxima de mensajes que se almacenarán antes de procesarlos
        
        Raises:
            redis.ConnectionError: Si no se puede establecer la conexión con Redis
        """
        try:
            # Inicializar el cliente Redis con decodificación automática de respuestas
            # decode_responses=True convierte automáticamente las respuestas de bytes a strings
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True
            )
            
            # Almacenar el tamaño máximo del buffer para controlar cuándo procesar los datos
            self.buffer_size = buffer_size
            
            # Clave que se usará en Redis para almacenar la lista de mensajes
            self.buffer_key = "message_buffer"
            
            # Verificar que la conexión está activa mediante un ping
            self.redis_client.ping()
            logger.info(f"✅ Conexión exitosa a Redis en {host}:{port}")
            print(f"✅ Conexión exitosa a Redis en {host}:{port}")
            
            # Limpiar cualquier dato residual del buffer al inicio
            # Esto previene que datos antiguos se mezclen con los nuevos
            self.redis_client.delete(self.buffer_key)
            
        except redis.ConnectionError as e:
            # Registrar el error y propagarlo
            logger.error(f"❌ Error al conectar con Redis: {e}")
            print(f"❌ Error al conectar con Redis: {e}")
            raise e

    def add_to_buffer(self, data: Dict[str, Any]) -> bool:
        """
        Añade un nuevo mensaje al buffer de Redis utilizando RPUSH.
        
        Args:
            data (Dict[str, Any]): Diccionario con los datos a almacenar
            
        Returns:
            bool: True si el buffer ha alcanzado su capacidad máxima, False en caso contrario
        
        Raises:
            Exception: Si ocurre algún error al interactuar con Redis
        """
        try:
            # RPUSH añade el elemento al final de la lista
            # Convertimos el diccionario a string para almacenarlo
            self.redis_client.rpush(self.buffer_key, str(data))
            
            # Obtener la longitud actual del buffer
            buffer_length = self.redis_client.llen(self.buffer_key)
            
            # Indicar si el buffer está lleno y debe ser procesado
            return buffer_length >= self.buffer_size
            
        except Exception as e:
            logger.error(f"❌ Error al añadir datos al buffer de Redis: {e}")
            print(f"❌ Error al añadir datos al buffer de Redis: {e}")
            raise e

    def get_buffer_batch(self) -> List[Dict[str, Any]]:
        """
        Recupera y elimina todos los mensajes actualmente en el buffer.
        Este método implementa un patrón de "leer y eliminar" para evitar
        el procesamiento duplicado de mensajes.
        
        Returns:
            List[Dict[str, Any]]: Lista de diccionarios con los datos almacenados
        
        Raises:
            Exception: Si ocurre algún error al interactuar con Redis
        """
        try:
            # Obtener la cantidad de elementos en el buffer
            batch_size = self.redis_client.llen(self.buffer_key)
            if batch_size == 0:
                return []

            # Lista para almacenar los datos procesados
            batch_data = []
            
            # LPOP extrae y elimina elementos desde el inicio de la lista
            # Procesamos elemento por elemento para transformarlos de string a diccionario
            for _ in range(batch_size):
                item = self.redis_client.lpop(self.buffer_key)
                if item:
                    # eval() convierte el string que representa un diccionario
                    # de nuevo en un diccionario de Python
                    batch_data.append(eval(item))
            
            logger.info(f"📤 Recuperados {len(batch_data)} elementos del buffer")
            return batch_data
            
        except Exception as e:
            logger.error(f"❌ Error al obtener batch del buffer de Redis: {e}")
            print(f"❌ Error al obtener batch del buffer de Redis: {e}")
            raise e

    def close(self):
        """
        Realiza la limpieza y cierre de la conexión con Redis.
        Elimina todos los datos del buffer y cierra la conexión de manera segura.
        
        Raises:
            Exception: Si ocurre algún error durante el proceso de cierre
        """
        try:
            # Eliminar todos los datos pendientes en el buffer
            self.redis_client.delete(self.buffer_key)
            
            # Cerrar la conexión con Redis
            self.redis_client.close()
            
            logger.info("✅ Conexión de Redis cerrada correctamente")
            print("✅ Conexión de Redis cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error al cerrar la conexión de Redis: {e}")
            print(f"❌ Error al cerrar la conexión de Redis: {e}")
            raise e