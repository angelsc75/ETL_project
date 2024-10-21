import psycopg2

# Datos de conexión
host = "localhost"
database = "hrpro"
user = "postgres"
password = "1234"
port = "5432"  # Este es el puerto por defecto de PostgreSQL

try:
    # Establecer la conexión
    connection = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )

    # Crear un cursor para ejecutar consultas
    cursor = connection.cursor()

    # Ejemplo de ejecución de una consulta
    cursor.execute("SELECT version();")

    # Obtener el resultado de la consulta
    db_version = cursor.fetchone()
    print(f"Versión de la base de datos: {db_version}")

    # Cerrar el cursor y la conexión
    cursor.close()
    connection.close()

except Exception as error:
    print(f"Error al conectar a la base de datos: {error}")