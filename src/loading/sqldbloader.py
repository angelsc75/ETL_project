import psycopg2

class SQLloader:
    def __init__(self, host="localhost", database="hrpro", user="postgres", password="1234", port="5432"):
        """
        Inicializa los parámetros de conexión, pero no conecta inmediatamente.
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None

    def load_to_sql(self, data):
        """
        Inserta un registro en Postgres.
        """
        try:
            # Conectar a la base de datos
            self.connect()
            # Ejecutar una consulta y mostrar el resultado
            self.execute_query("INSERT INTO hrpro.salarios (IBAN, salary, currency ) VALUES('ljhlkadfjhlkjhlkjh', 2222, '€');")
            # Cerrar la conexión
            self.close()

            print("Datos guardados en Postgres")
        except Exception as e:
            print(f"Ocurrió un error al insertar el registro: {e}")
            raise e  # Relanzar la excepción para manejarla en el flujo principal
        
    def connect(self):
        """
        Establece la conexión con la base de datos.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Conexión establecida exitosamente.")
        except Exception as error:
            print(f"Error al conectar a la base de datos: {error}")
            return None

    def close(self):
        """
        Cierra la conexión y el cursor.
        """
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()
        print("Conexión cerrada.")

    def execute_query(self, query):
        """
        Ejecuta una consulta y devuelve los resultados.
        """
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as error:
            print(f"Error al ejecutar la consulta: {error}")
            return None