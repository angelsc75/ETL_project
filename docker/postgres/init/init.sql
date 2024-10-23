-- Eliminar el esquema existente (si quieres recrear todo desde cero en cada inicio)
DROP SCHEMA IF EXISTS hrpro CASCADE;

-- Crear el esquema hrpro
CREATE SCHEMA hrpro;

-- Crear tabla de datos personales
CREATE TABLE IF NOT EXISTS hrpro.personal_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    last_name VARCHAR(100),
    fullname VARCHAR(200) UNIQUE,
    sex CHAR(1),
    telfnumber VARCHAR(20),
    passport VARCHAR(50) UNIQUE,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Crear tabla de datos de ubicación
CREATE TABLE IF NOT EXISTS hrpro.location_data (
    id SERIAL PRIMARY KEY,
    fullname VARCHAR(200),
    city VARCHAR(100),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fullname) REFERENCES hrpro.personal_data(fullname),
    UNIQUE (fullname)
);

-- Crear tabla de datos profesionales
CREATE TABLE IF NOT EXISTS hrpro.professional_data (
    id SERIAL PRIMARY KEY,
    fullname VARCHAR(200),
    company VARCHAR(200),
    company_address TEXT,
    company_telfnumber VARCHAR(20),
    company_email VARCHAR(100),
    job VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fullname) REFERENCES hrpro.personal_data(fullname),
    UNIQUE (fullname)
);

-- Crear tabla de datos bancarios
CREATE TABLE IF NOT EXISTS hrpro.bank_data (
    id SERIAL PRIMARY KEY,
    passport VARCHAR(50),
    IBAN VARCHAR(50),
    salary DECIMAL(12,2),
    currency VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (passport) REFERENCES hrpro.personal_data(passport),
    UNIQUE (passport)
);

-- Crear tabla de datos de red
CREATE TABLE IF NOT EXISTS hrpro.net_data (
    id SERIAL PRIMARY KEY,
    address TEXT,
    IPv4 VARCHAR(15),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (address)
);

-- Crear índices
CREATE INDEX IF NOT EXISTS idx_personal_data_fullname ON hrpro.personal_data(fullname);
CREATE INDEX IF NOT EXISTS idx_personal_data_passport ON hrpro.personal_data(passport);
CREATE INDEX IF NOT EXISTS idx_location_data_fullname ON hrpro.location_data(fullname);
CREATE INDEX IF NOT EXISTS idx_professional_data_fullname ON hrpro.professional_data(fullname);
CREATE INDEX IF NOT EXISTS idx_bank_data_passport ON hrpro.bank_data(passport);
CREATE INDEX IF NOT EXISTS idx_net_data_address ON hrpro.net_data(address);

-- Asegurar permisos (opcional)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA hrpro TO postgres;
