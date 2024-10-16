import json
from pymongo import MongoClient
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from collections import defaultdict

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

# Crear tablas en MySQL si no existen
# Tabla para location (debe crearse primero)
create_location_table = """
CREATE TABLE IF NOT EXISTS location (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fullname VARCHAR(200),
    city VARCHAR(100),
    address VARCHAR(200),
    UNIQUE KEY unique_fullname (fullname)
);
"""
mysql_cursor.execute(create_location_table)

# Añadir índice a la columna address en la tabla location
add_address_index = """
CREATE INDEX idx_address ON location(address);
"""
mysql_cursor.execute(add_address_index)

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
    location_id INT,  -- Permitimos que pueda ser NULL si no hay coincidencia
    data_validity VARCHAR(50),
    FOREIGN KEY (location_id) REFERENCES location(id)
);
"""
mysql_cursor.execute(create_personal_data_table)

create_passport_index = """
CREATE INDEX idx_passport_personal_data ON personal_data(passport);
"""
mysql_cursor.execute(create_passport_index)

create_fullname_location_index = """
CREATE INDEX idx_fullname_location ON location(fullname);
"""
mysql_cursor.execute(create_fullname_location_index)

create_fullname_personal_data_index = """
CREATE INDEX idx_fullname_personal_data ON personal_data(fullname);
"""
mysql_cursor.execute(create_fullname_personal_data_index)

create_professional_data_table = """
CREATE TABLE IF NOT EXISTS professional_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fullname VARCHAR(200),
    company VARCHAR(100),
    company_address VARCHAR(200),
    company_telfnumber VARCHAR(100),
    company_email VARCHAR(100),
    job VARCHAR(200),
    location_id INT,
    UNIQUE KEY unique_fullname (fullname),
    FOREIGN KEY (location_id) REFERENCES location(id)
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

# Diccionario para contar las ocurrencias de pasaportes
passport_counter = defaultdict(int)

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
            
            # Incrementar el contador de pasaportes y verificar la validez
            with lock:
                passport_counter[passport] += 1
                data_validity = "ok" if passport_counter[passport] == 1 else "aviso posible fraude"
            
            personal_data.append((name, last_name, fullname, sex, telfnumber, passport, email, data_validity))
        
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
        if location_data:
            mysql_cursor.executemany("""
                INSERT IGNORE INTO location 
                (fullname, city, address) 
                VALUES (%s, %s, %s)
            """, location_data)

        if personal_data:
            for person in personal_data:
                fullname = person[2]
        
        # Buscar coincidencias en location usando LIKE
        mysql_cursor.execute("""
            SELECT id FROM location 
            WHERE fullname LIKE CONCAT('%', %s, '%') 
            ORDER BY CHAR_LENGTH(fullname) ASC
        """, (fullname,))
        
        # Esto asegura que el resultado es consumido
        resultados = mysql_cursor.fetchall()
        if resultados:
            location_id = resultados[0][0]
        
                           
            mysql_cursor.execute("""
                INSERT INTO personal_data 
                (name, last_name, fullname, sex, telfnumber, passport, email, location_id, data_validity) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, person[:7] + (location_id, person[7]))
        else:
            # Si no se encuentra una coincidencia, se deja location_id como NULL
            mysql_cursor.execute("""
                INSERT INTO personal_data 
                (name, last_name, fullname, sex, telfnumber, passport, email, data_validity) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, person[:7] + (person[7],)) 
        
        if professional_data:
            for prof_data in professional_data:
                fullname = prof_data[0]
                if fullname != 'ERROR_code23a34j!#':
                    mysql_cursor.execute("SELECT id FROM location WHERE fullname = %s", (fullname,))
                    location_id = mysql_cursor.fetchone()
                    if location_id:
                        mysql_cursor.execute("""
                            INSERT IGNORE INTO professional_data
                            (fullname, company, company_address, company_telfnumber, company_email, job, location_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, prof_data + (location_id[0],))
                else:
                    mysql_cursor.execute("""
                        INSERT IGNORE INTO professional_data
                        (fullname, company, company_address, company_telfnumber, company_email, job)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, prof_data)
            
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

# Cerrar conexiones
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()

print("ETL process completed successfully")