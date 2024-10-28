from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import Column, Integer, Float, String, create_engine, or_, func, text
from sqlalchemy.future import select
import os
from dotenv import main

# Cargar variables de entorno
main.load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Obtener configuración de base de datos de las variables de entorno
db_type = 'postgresql+asyncpg'
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_DB')
db_schema = os.getenv('DB_SCHEMA')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')

# Construir URL de conexión a la base de datos
DATABASE_URL = f"{db_type}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

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
        try:
            yield session
        finally:
            await session.close()

# Definir tu modelo basado en la vista de PostgreSQL
class MiVista(Base):
    __tablename__ = 'person_full_data'  # Nombre de la vista
    __table_args__ = {'schema': db_schema}  # Esquema donde se encuentra la vista

    personal_id = Column(Integer, primary_key=True)
    name = Column(String)
    last_name = Column(String)
    fullname = Column(String)
    sex = Column(String)
    telfnumber = Column(String)
    passport = Column(String)
    email = Column(String)
    
    location_city = Column(String)
    location_address = Column(String)

    company_name = Column(String)
    company_address = Column(String)
    company_telfnumber = Column(String)
    company_email = Column(String)
    job_title = Column(String)

    bank_iban = Column(String)
    salary_amount = Column(Integer)
    salary_currency = Column(String)

    network_ipv4 = Column(String)
    

    

# Crear la instancia de FastAPI
app = FastAPI()

@app.get("/health")
async def health_check():
    try:
        # Verificar conexión con PostgreSQL
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Ruta para la raíz del servidor
@app.get("/")
async def read_root():
    return {"message": "Bienvenido a la API de FastAPI con PostgreSQL"}

# Nueva ruta para obtener el conteo de registros
@app.get("/count")
async def get_row_count(db: AsyncSession = Depends(get_db)):
    try:
        query = select(func.count()).select_from(MiVista)
        result = await db.execute(query)
        count = result.scalar()
        return {"total_rows": count}
    except Exception as e:
        return {"error": str(e)}

# Ruta para obtener todos los datos de la vista
@app.get("/vista")
async def get_data_from_view(db: AsyncSession = Depends(get_db)):
    try:
        query = select(MiVista).limit(100)
        result = await db.execute(query)
        return result.scalars().all()
    except Exception as e:
        return {"error": str(e)}

# Ruta para búsquedas
@app.get("/search")
async def search_data(
    search_term: str = Query(None, description="Término de búsqueda"),
    db: AsyncSession = Depends(get_db)
):
    try:
        if not search_term:
            return []
            
        query = select(MiVista).where(
            or_(
                MiVista.name.ilike(f"%{search_term}%"),
                MiVista.last_name.ilike(f"%{search_term}%"),
                MiVista.fullname.ilike(f"%{search_term}%"),
                MiVista.telfnumber.ilike(f"%{search_term}%"),
                MiVista.passport.ilike(f"%{search_term}%")
            )
        ).limit(100)
        
        result = await db.execute(query)
        return result.scalars().all()
    except Exception as e:
        return {"error": str(e)}