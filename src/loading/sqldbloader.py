import pg8000
import logging

class SQLloader:
    def __init__(self, host, database, user, password, port):
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

        # Configurar logging para seguir eventos y errores
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def load_to_sql(self, data_batch):
        """
        Inserta registros en las tablas correspondientes basados en el contenido del batch.
        """
        try:
            # Conectar a la base de datos
            self.connect()

            # Inicializar listas para cada tipo de dato
            personal_data = []
            location_data = []
            professional_data = []
            bank_data = []
            net_data = []

            # Clasificar los registros según los datos que contienen
            for data in data_batch:
                # Personal Data
                if "sex" in data:
                    print(f"LASTNAME: {data.get('last_name')}")
                    personal_data.append((
                        data.get("name"), 
                        data.get("last_name"), 
                        data.get("fullname"), 
                        data.get("sex"), 
                        data.get("telfnumber"), 
                        data.get("passport"), 
                        data.get("email")
                    ))

                # Location Data
                if "fullname" in data and "address" in data and "city" in data:
                    location_data.append((
                        data.get("fullname"), 
                        data.get("city"), 
                        data.get("address")
                    ))

                # Professional Data
                if "fullname" in data and "company" in data:
                    professional_data.append((
                        data.get("fullname"), 
                        data.get("company"), 
                        data.get("company_address"), 
                        data.get("company_telfnumber"), 
                        data.get("company_email"), 
                        data.get("job")
                    ))

                # Bank Data
                if "passport" in data and "IBAN" in data:
                    bank_data.append((
                        data.get("passport"), 
                        data.get("IBAN"), 
                        data.get("salary"),
                        data.get("currency")
                    ))

                # Net Data
                if "address" in data and "IPv4" in data:
                    net_data.append((
                        data.get("address"), 
                        data.get("IPv4")
                    ))

            # Insertar los datos en las tablas correspondientes
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

            # Confirmar las transacciones
            self.connection.commit()
            self.logger.info(f"{len(data_batch)} registros procesados correctamente.")
            print("Datos guardados en Postgres")
        except Exception as e:
            self.logger.error(f"Ocurrió un error al insertar los registros: {e}")
            self.connection.rollback()  # Hacer rollback si ocurre algún error
            raise e  # Relanzar la excepción para manejarla en el flujo principal

        finally:
            # Cerrar la conexión
            self.close()

    def insert_personal_data(self, personal_data):
        """
        Inserta datos personales en la tabla 'personal_data'.
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
        self.logger.info(f"{len(personal_data)} registros de datos personales insertados.")

    def insert_location_data(self, location_data):
        """
        Inserta datos de ubicación en la tabla 'location_data'.
        """
        query = """
        INSERT INTO hrpro.location_data (fullname, city, address)
        VALUES (%s, %s, %s)
        ON CONFLICT (fullname) DO UPDATE
        SET address = EXCLUDED.address,
            city = EXCLUDED.city;
        """
        self.cursor.executemany(query, location_data)
        self.logger.info(f"{len(location_data)} registros de datos de ubicación insertados.")

    def insert_professional_data(self, professional_data):
        """
        Inserta datos profesionales en la tabla 'professional_data'.
        """
        query = """
        INSERT INTO hrpro.professional_data (fullname, company, company_address, company_telfnumber, company_email, job)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (fullname) DO UPDATE 
        SET company = EXCLUDED.company,
            company_address = EXCLUDED.company_address,
            company_telfnumber = EXCLUDED.company_telfnumber,
            company_email = EXCLUDED.company_email,
            job = EXCLUDED.job;
        """
        self.cursor.executemany(query, professional_data)
        self.logger.info(f"{len(professional_data)} registros de datos profesionales insertados.")

    def insert_bank_data(self, bank_data):
        """
        Inserta datos bancarios en la tabla 'bank_data'.
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
        self.logger.info(f"{len(bank_data)} registros bancarios insertados.")

    def insert_net_data(self, net_data):
        """
        Inserta datos de red en la tabla 'net_data'.
        """
        query = """
        INSERT INTO hrpro.net_data (address, IPv4)
        VALUES (%s, %s)
        ON CONFLICT (address) DO UPDATE
        SET IPv4 = EXCLUDED.IPv4;
        """
        self.cursor.executemany(query, net_data)
        self.logger.info(f"{len(net_data)} registros de datos de red insertados.")

    def connect(self):
        """
        Establece la conexión con la base de datos usando pg8000.
        """
        try:
            self.connection = pg8000.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            self.logger.info("Conexión establecida exitosamente.")
        except Exception as error:
            self.logger.error(f"Error al conectar a la base de datos: {error}")
            raise error

    def close(self):
        """
        Cierra la conexión y el cursor.
        """
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()
        self.logger.info("Conexión cerrada.")