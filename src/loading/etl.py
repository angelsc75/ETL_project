import json
from pymongo import MongoClient
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
import threading
import time

# Lock para sincronización de threads
lock = threading.Lock()
# Conexión a MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['hr_data']
mongo_collection = mongo_db['hr_data']

# Conexión a MySQL
mysql_conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="admin",
    database="hr_data"
)
mysql_cursor = mysql_conn.cursor()

# Borrar y recrear la base de datos hr_data
mysql_cursor.execute("DROP DATABASE IF EXISTS hr_data")
mysql_cursor.execute("CREATE DATABASE hr_data")
mysql_cursor.execute("USE hr_data")

# Reconectar a la base de datos hr_data
mysql_conn.close()
mysql_conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="admin",
    database="hr_data"
)
mysql_cursor = mysql_conn.cursor()

# Crear tabla en MySQL si no existe
# Tabla para datos personales
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
);
"""
mysql_cursor.execute(create_personal_data_table)

create_location_table = """
CREATE TABLE IF NOT EXISTS location (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fullname VARCHAR(200),
    city VARCHAR(100),
    address VARCHAR(200),
    UNIQUE KEY unique_fullname_address (fullname, address)
);
"""
mysql_cursor.execute(create_location_table)

# Añadir índice a la columna address en la tabla location
add_address_index = """
CREATE INDEX idx_address ON location(address);
"""
mysql_cursor.execute(add_address_index)

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
mysql_cursor.execute(create_professional_data_table)

create_bank_data_table = """
CREATE TABLE IF NOT EXISTS bank_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    passport VARCHAR(20),
    iban VARCHAR(34),
    salary DECIMAL(10, 2),
    FOREIGN KEY (passport) REFERENCES personal_data(passport)
);
"""
mysql_cursor.execute(create_bank_data_table)

create_net_data_table = """
CREATE TABLE IF NOT EXISTS net_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    address VARCHAR(200),
    ipv4 VARCHAR(15),
    FOREIGN KEY (address) REFERENCES location(address)
);
"""
mysql_cursor.execute(create_net_data_table)

def determinar_tipo_dato(data):
    print("Data received:", data)
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

def process_batch(batch, thread_id):
    print(f"Thread {thread_id} starting to process batch of {len(batch)} items")
    start_time = time.time()
    
    personal_data = []
    location_data = []
    professional_data = []
    bank_data = []
    net_data = []
    
    for documento in batch:
        data = json.loads(documento['data'])
        tipo = determinar_tipo_dato(data)
        
        if tipo == 'personal':
            name = data.get('name', '').strip() or "unknown"
            last_name = data.get('last_name', '').strip() or "unknown"
            fullname = f"{name} {last_name}".strip()
            sex = data.get('sex')
            if isinstance(sex, list):
                sex = sex[0] if sex else None
            elif not isinstance(sex, str):
                sex = None
                
            telfnumber = data.get('telfnumber') if isinstance(data.get('telfnumber'), str) else None
            passport = data.get('passport') if isinstance(data.get('passport'), str) else None
            email = data.get('email') if isinstance(data.get('email'), str) else None
            
            personal_data.append((name, last_name, fullname, sex, telfnumber, passport, email))
        
        elif tipo == 'location':
            fullname = data.get('fullname', '').strip()
            city = data.get('city') if isinstance(data.get('city'), str) else None
            address = data.get('address') if isinstance(data.get('address'), str) else None
            location_data.append((fullname, city, address))
        
        elif tipo == 'profesional':
            fullname = data.get('fullname', '').strip()
            company = data.get('company') if isinstance(data.get('company'), str) else None
            company_address = data.get('company address') if isinstance(data.get('company address'), str) else None
            company_telfnumber = data.get('company_telfnumber') if isinstance(data.get('company_telfnumber'), str) else None
            company_email = data.get('company_email') if isinstance(data.get('company_email'), str) else None
            job = data.get('job') if isinstance(data.get('job'), str) else None
            professional_data.append((fullname, company, company_address, company_telfnumber, company_email, job))
        
        elif tipo == 'bank':
            passport = data.get('passport') if isinstance(data.get('passport'), str) else None
            iban = data.get('IBAN') if isinstance(data.get('IBAN'), str) else None
            salary = data.get('salary')
            try:
                salary = float(salary) if salary else None
            except ValueError:
                salary = None
            bank_data.append((passport, iban, salary))
        
        elif tipo == 'net':
            address = data.get('address') if isinstance(data.get('address'), str) else None
            ipv4 = data.get('IPv4') if isinstance(data.get('IPv4'), str) else None
            net_data.append((address, ipv4))

    with lock:
        if personal_data:
            mysql_cursor.executemany("""
                INSERT IGNORE INTO personal_data 
                (name, last_name, fullname, sex, telfnumber, passport, email) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, personal_data)
        
        if location_data:
            mysql_cursor.executemany("""
                INSERT IGNORE INTO location 
                (fullname, city, address) 
                VALUES (%s, %s, %s)
            """, location_data)
            
        if professional_data:
            mysql_cursor.executemany("""
                INSERT IGNORE INTO professional_data
                (fullname, company, company_address, company_telfnumber, company_email, job)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, professional_data)
            
        if bank_data:
            mysql_cursor.executemany("""
                INSERT IGNORE INTO bank_data
                (passport, iban, salary)
                VALUES(%s, %s, %s)
            """, bank_data)
                
        if net_data:
            mysql_cursor.executemany("""
                INSERT IGNORE INTO net_data
                (address, ipv4)
                VALUES(%s, %s)
            """, net_data)
        
        mysql_conn.commit()
    
    end_time = time.time()
    print(f"Thread {thread_id} finished processing batch in {end_time - start_time:.2f} seconds")

def procesar_datos():
    batch_size = 1000
    batch = []
    thread_count = 0
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        for documento in mongo_collection.find():
            batch.append(documento)
            if len(batch) >= batch_size:
                thread_count += 1
                executor.submit(process_batch, batch, thread_count)
                batch = []
         
        if batch:
            thread_count += 1
            executor.submit(process_batch, batch, thread_count)
    
    print(f"All {thread_count} threads have been submitted for processing")
    
    # Esperar a que todos los hilos terminen
    executor.shutdown(wait=True)
    print("All threads have completed execution")

procesar_datos()

# Crear relaciones adicionales
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS personal_location (
        personal_id INT,
        location_id INT,
        FOREIGN KEY (personal_id) REFERENCES personal_data(id),
        FOREIGN KEY (location_id) REFERENCES location(id),
        PRIMARY KEY (personal_id, location_id)
    )
""")

mysql_cursor.execute("""
    INSERT IGNORE INTO personal_location (personal_id, location_id)
    SELECT pd.id, l.id
    FROM personal_data pd
    JOIN location l ON INSTR(l.fullname, pd.fullname) > 0
""")

mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS personal_professional (
        personal_id INT,
        professional_id INT,
        FOREIGN KEY (personal_id) REFERENCES personal_data(id),
        FOREIGN KEY (professional_id) REFERENCES professional_data(id),
        PRIMARY KEY (personal_id, professional_id)
    )
""")

mysql_cursor.execute("""
    INSERT IGNORE INTO personal_professional (personal_id, professional_id)
    SELECT pd.id, pr.id
    FROM personal_data pd
    JOIN professional_data pr ON INSTR(pr.fullname, pd.fullname) > 0
""")

mysql_conn.commit()

# Cerrar conexiones
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()

print("ETL process completed successfully")