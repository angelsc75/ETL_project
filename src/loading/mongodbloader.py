from pymongo import MongoClient

class MongoDBLoader:
    def __init__(self, uri="mongodb://adminUser:12345@localhost:27017/hr_data", db_name="hr_data", collection_name="data"):
        """
        Inicializa la conexión con MongoDB.
        
        :param uri: URI de conexión a MongoDB.
        :param db_name: Nombre de la base de datos.
        :param collection_name: Nombre de la colección.
        """
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def load_to_mongodb(self, data):
        """
        Inserta un documento en MongoDB.
        
        :param data: Diccionario con los datos a insertar en MongoDB.
        :return: ID del documento insertado.
        """
        try:
            result = self.collection.insert_many(data)
            print(f"Datos guardados en MongoDB")
            # print(f"Datos guardados en MongoDB con el ID: {result.inserted_ids}")
        except Exception as e:
            print(f"Ocurrió un error al insertar el documento: {e}")
            raise e  # Relanzar la excepción para manejarla en el flujo principal

    def close(self):
        """
        Cierra la conexión con MongoDB.
        """
        self.client.close()
