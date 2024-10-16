import json
import pymongo
import mysql.connector
from pymongo import MongoClient

# Conexión a MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['hr_data']
mongo_collection = mongo_db['hr_data']

# Conexión a MySQL
mysql_conn = mysql.connector.connect(
    host="localhost",  # No incluya el puerto aquí
    port=3306,  # Especifique el puerto por separado
    user="root",
    password="admin",
    database="hr_mysql_g7"
)
mysql_cursor = mysql_conn.cursor()

# Función para transformar los datos
def transform_data(data):
    transformed = {}
    
    # Manejar nombre y apellido
    transformed['fullname'] = f"{data.get('name', '')} {data.get('last_name', '')}".strip()
    
    # Manejar sexo
    sex = data.get('sex', [])
    transformed['sex'] = sex[0] if isinstance(sex, list) and len(sex) > 0 else ''
    
    # Manejar número de teléfono
    telfnumber = data.get('telfnumber', '') or data.get('company_telfnumber', '')
    transformed['telfnumber'] = ''.join(filter(str.isdigit, telfnumber))
    
    # Manejar email
    transformed['email'] = data.get('email', '') or data.get('company_email', '')
    
    # Manejar campos adicionales
    transformed['passport'] = data.get('passport', '')
    transformed['company'] = data.get('company', '')
    transformed['company_address'] = data.get('company address', '')
    transformed['job'] = data.get('job', '')
    
    return transformed

# Crear tabla en MySQL si no existe
create_table_query = """
CREATE TABLE IF NOT EXISTS employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fullname VARCHAR(255),
    sex CHAR(2),
    telfnumber VARCHAR(20),
    email VARCHAR(255),
    passport VARCHAR(20),
    company VARCHAR(255),
    company_address VARCHAR(255),
    job VARCHAR(255)
)
"""
mysql_cursor.execute(create_table_query)

# Consulta MongoDB
for document in mongo_collection.find():
    # Extraer y parsear los datos
    data = json.loads(document['data'])
    
    # Transformar los datos
    transformed_data = transform_data(data)
    
    # Preparar la consulta SQL
    sql = """INSERT INTO employees 
             (fullname, sex, telfnumber, email, passport, company, company_address, job) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    values = (
        transformed_data['fullname'],
        transformed_data['sex'],
        transformed_data['telfnumber'],
        transformed_data['email'],
        transformed_data['passport'],
        transformed_data['company'],
        transformed_data['company_address'],
        transformed_data['job']
    )
    
    # Ejecutar la consulta SQL
    mysql_cursor.execute(sql, values)

# Commit los cambios y cerrar conexiones
mysql_conn.commit()
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()