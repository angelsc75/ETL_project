import json
from pymongo import MongoClient
import mysql.connector
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor
import threading
import time

# Configuraciones
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'hr_data'
MONGO_COLLECTION = 'hr_data'
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'admin'
}
BATCH_SIZE = 1000
PROCESSING_INTERVAL = 60  # segundos

# Lock para sincronización de threads
lock = threading.Lock()

def get_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_CONFIG['host'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database='hr_data'  # Selecciona la base de datos aquí
        )
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def setup_mysql_tables():
    conn = get_mysql_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        # Crear tablas
        create_personal_data_table = """
        CREATE TABLE IF NOT EXISTS personal_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            last_name VARCHAR(100),
            fullname VARCHAR(201),
            sex VARCHAR(10),
            telfnumber VARCHAR(100),
            passport VARCHAR(20),
            email VARCHAR(100),
            UNIQUE KEY unique_passport (passport),
            UNIQUE KEY unique_fullname (fullname)
        )
        """
        cursor.execute(create_personal_data_table)

        create_location_table = """
        CREATE TABLE IF NOT EXISTS location (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fullname VARCHAR(200),
            city VARCHAR(100),
            address VARCHAR(200),
            UNIQUE KEY unique_fullname_address (fullname, address),
            INDEX idx_address (address)  -- Añadido el índice requerido
        );
        """
        cursor.execute(create_location_table)

        create_professional_data_table = """
        CREATE TABLE IF NOT EXISTS professional_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fullname VARCHAR(200),
            company VARCHAR(100),
            company_address VARCHAR(200),
            company_telfnumber VARCHAR(100),
            company_email VARCHAR(100),
            job VARCHAR(200),
            UNIQUE KEY unique_fullname (fullname)
        );
        """
        cursor.execute(create_professional_data_table)

        create_bank_data_table = """
        CREATE TABLE IF NOT EXISTS bank_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            passport VARCHAR(20),
            iban VARCHAR(34),
            salary DECIMAL(10, 2),
            FOREIGN KEY (passport) REFERENCES personal_data(passport)
        );
        """
        cursor.execute(create_bank_data_table)

        create_net_data_table = """
        CREATE TABLE IF NOT EXISTS net_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            address VARCHAR(200),
            ipv4 VARCHAR(15),
            FOREIGN KEY (address) REFERENCES location(address)
        );
        """
        cursor.execute(create_net_data_table)

        conn.commit()
    except Error as e:
        print(f"Error setting up MySQL tables: {e}")
    finally:
        cursor.close()
        conn.close()

def process_batch(batch):
    conn = get_mysql_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        personal_data = []
        location_data = []
        professional_data = []
        bank_data = []
        net_data = []

        for documento in batch:
            data = documento['data']
            tipo = determinar_tipo_dato(data)

            if tipo == 'personal':
                personal_data.append((data['name'], data['last_name'], f"{data['name']} {data['last_name']}", 
                                      data.get('sex'), data.get('telfnumber'), data.get('passport'), data.get('email')))
            elif tipo == 'location':
                location_data.append((data['fullname'], data.get('city'), data.get('address')))
            elif tipo == 'profesional':
                professional_data.append((data['fullname'], data.get('company'), data.get('company_address'),
                                          data.get('company_telfnumber'), data.get('company_email'), data.get('job')))
            elif tipo == 'bank':
                bank_data.append((data.get('passport'), data.get('IBAN'), data.get('salary')))
            elif tipo == 'net':
                net_data.append((data.get('address'), data.get('IPv4')))

        with lock:
            if personal_data:
                cursor.executemany("""
                INSERT IGNORE INTO personal_data 
                (name, last_name, fullname, sex, telfnumber, passport, email) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, personal_data)

            if location_data:
                cursor.executemany("""
                INSERT IGNORE INTO location 
                (fullname, city, address) 
                VALUES (%s, %s, %s)
            """, location_data)

            if professional_data:
                cursor.executemany("""
                INSERT IGNORE INTO professional_data
                (fullname, company, company_address, company_telfnumber, company_email, job)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, professional_data)

            if bank_data:
                cursor.executemany("""
                INSERT IGNORE INTO bank_data
                (passport, iban, salary)
                VALUES(%s, %s, %s)
            """, bank_data)

            if net_data:
                cursor.executemany("""
                INSERT IGNORE INTO net_data
                (address, ipv4)
                VALUES(%s, %s)
            """, net_data)

        conn.commit()
    except Error as e:
        print(f"Error processing batch: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def determinar_tipo_dato(data):
    if 'name' in data and 'last_name' in data:
        return 'personal'
    elif 'fullname' in data and 'city' in data:
        return 'location'
    elif 'company' in data:
        return 'profesional'
    elif 'IBAN' in data:
        return 'bank'
    elif 'IPv4' in data:
        return 'net'
    else:
        return 'desconocido'

def update_relations():
    conn = get_mysql_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        with lock:
            cursor.execute("""
            INSERT IGNORE INTO personal_location (personal_id, location_id)
            SELECT pd.id, MIN(l.id)
            FROM personal_data pd
            LEFT JOIN personal_location pl ON pd.id = pl.personal_id
            JOIN location l ON l.fullname LIKE CONCAT(pd.name, ' ', pd.last_name, '%')
            WHERE pl.personal_id IS NULL
            GROUP BY pd.id
        """)

            cursor.execute("""
            INSERT IGNORE INTO personal_professional (personal_id, professional_id)
            SELECT pd.id, MIN(pr.id)
            FROM personal_data pd
            LEFT JOIN personal_professional pp ON pd.id = pp.personal_id
            JOIN professional_data pr ON pr.fullname LIKE CONCAT(pd.name, ' ', pd.last_name, '%')
            WHERE pp.personal_id IS NULL
            GROUP BY pd.id
        """)
        conn.commit()
    except Error as e:
        print(f"Error updating relations: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def process_mongo_to_mysql():
    try:
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB]
        mongo_collection = mongo_db[MONGO_COLLECTION]

        last_processed_id = None
        with ThreadPoolExecutor(max_workers=4) as executor:  # Ajusta el número de workers según tus necesidades
            while True:
                query = {} if last_processed_id is None else {'_id': {'$gt': last_processed_id}}
                batch = list(mongo_collection.find(query).sort('_id', 1).limit(BATCH_SIZE))

                if not batch:
                    time.sleep(PROCESSING_INTERVAL)
                    continue

                # Procesar cada lote en paralelo
                future = executor.submit(process_batch, batch)
                future.add_done_callback(lambda f: update_relations())

                last_processed_id = batch[-1]['_id']
                print(f"Processed batch up to ID: {last_processed_id}")
    except Exception as e:
        print(f"Error processing data from MongoDB: {e}")
    finally:
        mongo_client.close()

if __name__ == "__main__":
    setup_mysql_tables()
    process_mongo_to_mysql()
