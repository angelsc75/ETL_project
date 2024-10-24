-- Establecer la codificación de caracteres a UTF8
\encoding UTF8

-- Conectarse a la base de datos postgres
\c postgres

-- Cerrar todas las conexiones activas a la base de datos 'hrpro'
SELECT pg_terminate_backend(pid)  
FROM pg_stat_activity
WHERE datname = 'hrpro' AND pid <> pg_backend_pid();

-- Conectarse a la base de datos 'hrpro'
\c hrpro

-- Establecer la codificación de cliente a UTF8
SET client_encoding TO 'UTF8';

-- Agregar un comentario descriptivo sobre la base de datos 'hrpro'
COMMENT ON DATABASE hrpro
    IS 'Base de datos para ETL HRPro';

-- Crear el esquema si no existe
CREATE SCHEMA IF NOT EXISTS hrpro;

-- -----------------------------  Eliminar las tablas si existen  ----------------------------- --
DROP TABLE IF EXISTS hrpro.personal_data CASCADE;
DROP TABLE IF EXISTS hrpro.location_data CASCADE;
DROP TABLE IF EXISTS hrpro.professional_data CASCADE;
DROP TABLE IF EXISTS hrpro.bank_data CASCADE;
DROP TABLE IF EXISTS hrpro.net_data CASCADE;

-- ======================================
-- Script para crear las tablas en PostgreSQL 
-- ======================================

-- Tabla de datos personales (Personal data)
CREATE TABLE hrpro.personal_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    last_name VARCHAR(100),
    fullname VARCHAR(200),
    sex CHAR(2),
    telfnumber VARCHAR(50),
    passport VARCHAR(20) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE,
    CONSTRAINT email_check CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

-- Tabla de ubicación (Location data)
CREATE TABLE hrpro.location_data (
    id SERIAL PRIMARY KEY,
    fullname VARCHAR(200) UNIQUE NOT NULL,
    city VARCHAR(100),
    address VARCHAR(255) NOT NULL  
);

-- Tabla de datos profesionales (Professional data)
CREATE TABLE hrpro.professional_data (
    id SERIAL PRIMARY KEY,
    fullname VARCHAR(200) UNIQUE NOT NULL,  -- Relación por 'fullname'
    company VARCHAR(255),
    company_address VARCHAR(255),
    company_telfnumber VARCHAR(50),
    company_email VARCHAR(255),
    job VARCHAR(255),
    CONSTRAINT company_email_check CHECK (company_email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

-- Tabla de datos bancarios (Bank Data)
CREATE TABLE hrpro.bank_data (
    id SERIAL PRIMARY KEY,
    passport VARCHAR(20) UNIQUE NOT NULL,
    IBAN VARCHAR(34) UNIQUE NOT NULL,
    salary NUMERIC,
    currency VARCHAR(5),
    CONSTRAINT iban_check CHECK (IBAN ~* '^[A-Za-z0-9]+$')
);

-- Tabla de datos de red (Net Data)
CREATE TABLE hrpro.net_data (
    id SERIAL PRIMARY KEY,
    address VARCHAR(255) UNIQUE NOT NULL,
    IPv4 VARCHAR(15) UNIQUE NOT NULL,
    CONSTRAINT ipv4_check CHECK (IPv4 ~* '^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
);

-- ======================================
-- Restricciones adicionales
-- ======================================

-- Restricción para asegurar que el email en la tabla personal_data tiene un formato válido
ALTER TABLE hrpro.personal_data ADD CONSTRAINT personal_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

-- Restricción para asegurar que el formato de IBAN en la tabla bank_data es válido
ALTER TABLE hrpro.bank_data ADD CONSTRAINT iban_format CHECK (IBAN ~* '^[A-Za-z0-9]+$');

-- Restricción para asegurar que la dirección IPv4 tiene un formato válido
ALTER TABLE hrpro.net_data ADD CONSTRAINT ipv4_format CHECK (IPv4 ~* '^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$');


-- ======================================
-- Script para crear la vista de personas
-- ======================================

CREATE OR REPLACE VIEW hrpro.person_full_data AS
SELECT 
    pd.id AS personal_id,
    pd.name,
    pd.last_name,
    pd.fullname,
    pd.sex,
    pd.telfnumber,
    pd.passport,
    pd.email,
    
    ld.city AS location_city,
    ld.address AS location_address,

    profd.company AS company_name,
    profd.company_address,
    profd.company_telfnumber,
    profd.company_email AS company_email,
    profd.job AS job_title,

    bd.IBAN AS bank_IBAN,
    bd.salary AS salary_amount,
    bd.currency AS salary_currency,

    nd.IPv4 AS network_IPv4

FROM hrpro.personal_data pd
LEFT JOIN hrpro.location_data ld ON pd.fullname = ld.fullname
LEFT JOIN hrpro.professional_data profd ON pd.fullname = profd.fullname
LEFT JOIN hrpro.bank_data bd ON pd.passport = bd.passport
LEFT JOIN hrpro.net_data nd ON ld.address = nd.address
WHERE pd.fullname IS NOT NULL AND pd.fullname <> '';
