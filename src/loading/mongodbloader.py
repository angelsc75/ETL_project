from pymongo import MongoClient
from src.logger import logger

class MongoDBLoader:
    """
    Clase para gestionar la carga de datos en MongoDB.
    Proporciona funcionalidades para conectar con MongoDB y realizar inserciones masivas de datos.
    """

    def __init__(self, uri: str, db_name: str, collection_name: str):
        """
        Inicializa la conexión con MongoDB utilizando los parámetros proporcionados.
        
        Args:
            uri (str): URI completa de conexión a MongoDB 
                      (ejemplo: "mongodb://usuario:contraseña@host:puerto/")
            db_name (str): Nombre de la base de datos a utilizar
            collection_name (str): Nombre de la colección donde se insertarán los datos
        """
        # Crear el cliente de MongoDB usando la URI proporcionada
        # MongoClient maneja automáticamente el pool de conexiones
        self.client = MongoClient(uri)
        
        # Obtener referencia a la base de datos
        # Esta operación no realiza ninguna conexión real hasta que se necesita
        self.db = self.client[db_name]
        
        # Obtener referencia a la colección
        # MongoDB creará la colección automáticamente si no existe
        self.collection = self.db[collection_name]

    def load_to_mongodb(self, data: list):
        """
        Realiza una inserción masiva de documentos en MongoDB.
        Utiliza insert_many para mejor rendimiento en comparación con inserciones individuales.
        
        Args:
            data (list): Lista de diccionarios donde cada diccionario es un documento a insertar
        
        Returns:
            None
        
        Raises:
            Exception: Cualquier error que ocurra durante la inserción es capturado y relanzado
        """
        try:
            # insert_many() es más eficiente que múltiples insert_one()
            # para insertar múltiples documentos
            result = self.collection.insert_many(data)
            
            # Registrar el éxito de la operación
            print("Datos guardados en MongoDB")
            logger.info("Datos guardados en MongoDB")
            
            # Nota: La siguiente línea está comentada pero puede ser útil para debugging
            # print(f"Datos guardados en MongoDB con el ID: {result.inserted_ids}")
            
        except Exception as e:
            # Registrar el error tanto en consola como en el log
            print(f"Ocurrió un error al insertar el documento: {e}")
            logger.error(f"Ocurrió un error al insertar el documento: {e}")
            
            # Relanzar la excepción para que pueda ser manejada en niveles superiores
            # Esto permite que el código que llama a esta función pueda implementar
            # su propia lógica de manejo de errores
            raise e

    def close(self):
        """
        Cierra la conexión con MongoDB de manera segura.
        Es importante llamar a este método cuando ya no se necesita la conexión
        para liberar recursos del sistema.
        """
        # close() liberará las conexiones en el pool y limpiará los recursos
        self.client.close()