# sqldbloader.py
"""
Este módulo implementa la clase SQLloader, que se encarga de gestionar la conexión
y las operaciones de inserción en una base de datos PostgreSQL.
La clase está diseñada para manejar diferentes tipos de datos (personal, ubicación, 
profesional, bancario y de red) y insertarlos en sus respectivas tablas.
"""

import psycopg2
from logger import logger

class SQLloader:
    def __init__(self, host, database, user, password, port):
        """
        Constructor de la clase SQLloader. Inicializa los parámetros de conexión
        pero no establece la conexión inmediatamente para evitar consumo innecesario
        de recursos.

        Args:
            host (str): Dirección del servidor de base de datos
            database (str): Nombre de la base de datos
            user (str): Usuario de la base de datos
            password (str): Contraseña del usuario
            port (int): Puerto de conexión
        """
        # Almacenamiento de credenciales y parámetros de conexión
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        # Inicialización de variables de conexión como None
        self.connection = None
        self.cursor = None

    def load_to_sql(self, data_batch):
        """
        Método principal que procesa un lote de datos y los distribuye en las tablas
        correspondientes de la base de datos.

        Args:
            data_batch (list): Lista de diccionarios, donde cada diccionario contiene
                             los datos a insertar en las diferentes tablas.

        Raises:
            Exception: Cualquier error durante el proceso de inserción
        """
        try:
            # Establecer conexión con la base de datos
            self.connect()

            # Inicialización de listas para cada tipo de dato
            # Cada lista almacenará tuplas con los valores a insertar
            personal_data = []    # Datos personales (nombre, apellido, etc.)
            location_data = []    # Datos de ubicación (ciudad, dirección)
            professional_data = [] # Datos profesionales (empresa, cargo)
            bank_data = []        # Datos bancarios (IBAN, salario)
            net_data = []         # Datos de red (IP, dirección)

            # Iteración sobre cada registro en el lote de datos
            for data in data_batch:
                # Clasificación de datos según los campos presentes
                
                # Procesamiento de datos personales
                if "sex" in data:  # Se usa 'sex' como campo clave para identificar datos personales
                    personal_data.append((
                        data.get("name"),        # Nombre
                        data.get("last_name"),   # Apellido
                        data.get("fullname"),    # Nombre completo
                        data.get("sex"),         # Género
                        data.get("telfnumber"),  # Teléfono
                        data.get("passport"),    # Número de pasaporte
                        data.get("email")        # Correo electrónico
                    ))

                # Procesamiento de datos de ubicación
                if "fullname" in data and "address" in data and "city" in data:
                    location_data.append((
                        data.get("fullname"),    # Nombre completo como clave de relación
                        data.get("city"),        # Ciudad
                        data.get("address")      # Dirección
                    ))

                # Procesamiento de datos profesionales
                if "fullname" in data and "company" in data:
                    professional_data.append((
                        data.get("fullname"),           # Nombre completo como clave de relación
                        data.get("company"),            # Nombre de la empresa
                        data.get("company_address"),    # Dirección de la empresa
                        data.get("company_telfnumber"), # Teléfono de la empresa
                        data.get("company_email"),      # Email de la empresa
                        data.get("job")                 # Cargo/puesto
                    ))

                # Procesamiento de datos bancarios
                if "passport" in data and "IBAN" in data:
                    bank_data.append((
                        data.get("passport"),    # Pasaporte como clave de relación
                        data.get("IBAN"),        # Número de cuenta IBAN
                        data.get("salary"),      # Salario
                        data.get("currency")     # Moneda del salario
                    ))

                # Procesamiento de datos de red
                if "address" in data and "IPv4" in data:
                    net_data.append((
                        data.get("address"),     # Dirección física
                        data.get("IPv4")         # Dirección IP
                    ))

            # Inserción de datos en las tablas correspondientes
            # Solo se llama a cada método si hay datos para insertar
            if personal_data:
                self.insert_personal_data(personal_data)
            if location_data:
                self.insert_location_data(location_data)
            if professional_data:
                self.insert_professional_data(professional_data)
            if bank_data:
                self.insert_bank_data(bank_data)
            if net_data:
                self.insert_net_data(net_data)

            # Confirmar todas las transacciones
            self.connection.commit()
            logger.info(f"{len(data_batch)} registros procesados correctamente.")
            print("Datos guardados en Postgres")

        except Exception as e:
            # En caso de error, registrar el error y hacer rollback
            logger.error(f"Ocurrió un error al insertar los registros: {e}")
            self.connection.rollback()  
            raise e  # Relanzar la excepción para su manejo en el nivel superior

        finally:
            # Asegurar que la conexión se cierre incluso si hay errores
            self.close()

    def insert_personal_data(self, personal_data):
        """
        Inserta o actualiza datos personales en la tabla 'personal_data'.
        Utiliza ON CONFLICT para actualizar registros existentes basándose en el pasaporte.

        Args:
            personal_data (list): Lista de tuplas con datos personales a insertar
        """
        query = """
        INSERT INTO hrpro.personal_data (name, last_name, fullname, sex, telfnumber, passport, email)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (passport) DO UPDATE
        SET name = EXCLUDED.name,
            last_name = EXCLUDED.last_name,
            fullname = EXCLUDED.fullname,
            sex = EXCLUDED.sex,
            telfnumber = EXCLUDED.telfnumber,
            email = EXCLUDED.email;
        """
        self.cursor.executemany(query, personal_data)
        logger.info(f"{len(personal_data)} registros de datos personales insertados.")

    def insert_location_data(self, location_data):
        """
        Inserta o actualiza datos de ubicación en la tabla 'location_data'.
        Utiliza ON CONFLICT para actualizar registros existentes basándose en el nombre completo.

        Args:
            location_data (list): Lista de tuplas con datos de ubicación a insertar
        """
        query = """
        INSERT INTO hrpro.location_data (fullname, city, address)
        VALUES (%s, %s, %s)
        ON CONFLICT (fullname) DO UPDATE
        SET address = EXCLUDED.address,
            city = EXCLUDED.city;
        """
        self.cursor.executemany(query, location_data)
        logger.info(f"{len(location_data)} registros de datos de ubicación insertados.")

    def insert_professional_data(self, professional_data):
        """
        Inserta o actualiza datos profesionales en la tabla 'professional_data'.
        Utiliza ON CONFLICT para actualizar registros existentes basándose en el nombre completo.

        Args:
            professional_data (list): Lista de tuplas con datos profesionales a insertar
        """
        query = """
        INSERT INTO hrpro.professional_data 
            (fullname, company, company_address, company_telfnumber, company_email, job)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (fullname) DO UPDATE 
        SET company = EXCLUDED.company,
            company_address = EXCLUDED.company_address,
            company_telfnumber = EXCLUDED.company_telfnumber,
            company_email = EXCLUDED.company_email,
            job = EXCLUDED.job;
        """
        self.cursor.executemany(query, professional_data)
        logger.info(f"{len(professional_data)} registros de datos profesionales insertados.")

    def insert_bank_data(self, bank_data):
        """
        Inserta o actualiza datos bancarios en la tabla 'bank_data'.
        Utiliza ON CONFLICT para actualizar registros existentes basándose en el pasaporte.

        Args:
            bank_data (list): Lista de tuplas con datos bancarios a insertar
        """
        query = """
        INSERT INTO hrpro.bank_data (passport, IBAN, salary, currency)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (passport) DO UPDATE 
        SET IBAN = EXCLUDED.IBAN,
            salary = EXCLUDED.salary,
            currency = EXCLUDED.currency;
        """
        self.cursor.executemany(query, bank_data)
        logger.info(f"{len(bank_data)} registros bancarios insertados.")

    def insert_net_data(self, net_data):
        """
        Inserta o actualiza datos de red en la tabla 'net_data'.
        Utiliza ON CONFLICT para actualizar registros existentes basándose en la dirección.

        Args:
            net_data (list): Lista de tuplas con datos de red a insertar
        """
        query = """
        INSERT INTO hrpro.net_data (address, IPv4)
        VALUES (%s, %s)
        ON CONFLICT (address) DO UPDATE
        SET IPv4 = EXCLUDED.IPv4;
        """
        self.cursor.executemany(query, net_data)
        logger.info(f"{len(net_data)} registros de datos de red insertados.")

    def connect(self):
        """
        Establece la conexión con la base de datos PostgreSQL.
        Inicializa tanto la conexión como el cursor para operaciones posteriores.

        Raises:
            Exception: Si hay algún error en la conexión
        """
        try:
            # Crear la conexión con los parámetros almacenados
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            # Crear el cursor para ejecutar queries
            self.cursor = self.connection.cursor()
            logger.info("Conexión establecida exitosamente.")
        except Exception as error:
            logger.error(f"Error al conectar a la base de datos: {error}")
            raise error

    def close(self):
        """
        Cierra la conexión y el cursor de la base de datos.
        Se asegura de cerrar primero el cursor y luego la conexión
        para evitar pérdida de recursos.
        """
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()
        logger.info("Conexión cerrada.")