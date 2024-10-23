from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import FastAPI, Depends
from sqlalchemy import Column, Integer, String
from sqlalchemy.future import select

import os
from dotenv import main

# Cargar variables de entorno
main.load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Obtener configuración de base de datos de las variables de entorno
db_type = os.getenv('DB_TYPE')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_DB')
db_schema = os.getenv('DB_SCHEMA')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')

# Construir URL de conexión a la base de datos
DATABASE_URL = f"{db_type}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

# Crear instancia de FastAPI
print(f"Conectando a la base de datos: {DATABASE_URL}")

# Crear motor de conexión a la base de datos
engine = create_async_engine(DATABASE_URL, echo=True)

# Crear una sesión asíncrona
async_session = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

# Declarative base para los modelos ORM
Base = declarative_base()

# Función para obtener sesión
async def get_db():
    async with async_session() as session:
        yield session

# Definir tu modelo basado en la vista de PostgreSQL
class MiVista(Base):
    __tablename__ = 'person_full_data'  # Nombre de la vista
    __table_args__ = {'schema': db_schema}  # Esquema donde se encuentra la vista

    personal_id = Column(Integer, primary_key=True)  # pd.id
    name = Column(String)  # pd.name
    last_name = Column(String)  # pd.last_name
    fullname = Column(String)  # pd.fullname
    sex = Column(String)  # pd.sex
    telfnumber = Column(String)  # pd.telfnumber
    passport = Column(String)  # pd.passport
    email = Column(String)  # pd.email

# Crear la instancia de FastAPI
app = FastAPI()

# Ruta para la raíz del servidor
@app.get("/")
async def read_root():
    return {"message": "Bienvenido a la API de FastAPI con PostgreSQL"}

# Ruta para obtener los datos de la vista
@app.get("/vista")
async def get_data_from_view(db: AsyncSession = Depends(get_db)):
    query = select(MiVista).limit(100)  # Limitar la consulta a los primeros 100 registros
    result = await db.execute(query)
    return result.scalars().all()