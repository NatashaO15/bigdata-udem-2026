-- =====================================================================
-- TALLER 2 · crear_tablas.sql · Equipo sinergia4 · Anfitriona: Natasha Orozco
-- Crea en Snowflake las tablas donde cargar_snowflake deja el gold: la del equipo y la de la casa.
-- Pregunta: ¿la lluvia cambia la concentración del mercado entre Uber y Lyft por zona
-- en Nueva York durante 2024?
--
-- Cuándo ejecutarlo: una vez antes de la primera corrida, y otra vez después de cada
-- resetear_entorno (el reset borra las tablas de BIGDATA.DEV).
-- Dónde: Snowsight, cuenta de la anfitriona, todo el archivo de una vez.
-- El CREATE TABLE es el mismo texto de DDL_SINERGIA4_CONCENTRACION_LLUVIA en src/pipeline/carga.py.
-- =====================================================================

USE ROLE BIGDATA_DEV;
USE WAREHOUSE BIGDATA_WH;
USE DATABASE BIGDATA;

-- ---------------------------------------------------------------------
-- 1. Tabla del equipo: una fila por fecha, zona de recogida y empresa.
--    Viene de gold/sinergia4_concentracion_lluvia (función viajes_por_zona_empresa_y_lluvia).
--    LLUVIA_MM y NIVEL_LLUVIA son del día: se repiten en todas las filas de esa fecha.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS BIGDATA.DEV.SINERGIA4_CONCENTRACION_LLUVIA (
    FECHA          DATE,          -- día de recogida (pickup_date)
    PU_LOCATION_ID NUMBER(38,0),  -- id de la zona de recogida (TLC)
    BOROUGH        VARCHAR,       -- borough de la zona; 'Desconocido' si no está en el lookup
    ZONA           VARCHAR,       -- nombre de la zona; 'Zona <id>' si no está en el lookup
    EMPRESA        VARCHAR,       -- Uber o Lyft
    LLUVIA_MM      FLOAT,         -- lluvia total del día en mm (Open-Meteo); nula si no hay dato
    NIVEL_LLUVIA   VARCHAR,       -- sin_lluvia (0) · ligera (>0 y <5) · fuerte (>=5) · sin_dato
    VIAJES         NUMBER(38,0),  -- viajes del grupo (filas de silver)
    INGRESO_TOTAL  FLOAT,         -- suma de total_pagado, en dólares
    PROPINA_TOTAL  FLOAT          -- suma de propinas, en dólares
);

-- ---------------------------------------------------------------------
-- 1b. Tabla de la casa (ejemplo de la clase 7). No responde la pregunta, pero el pipeline la
--     sigue calculando en la misma corrida y cargar_snowflake se detiene si no existe.
--     Es el texto de DDL_TARIFA_POR_MILLA en src/pipeline/carga.py.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS BIGDATA.DEV.TARIFA_POR_MILLA_BARRIO (
    FECHA              DATE,
    BOROUGH            VARCHAR,
    FRANJA             VARCHAR,
    VIAJES             NUMBER(38,0),
    DISTANCIA_TOTAL_MI FLOAT,
    INGRESO_TOTAL      FLOAT,
    TARIFA_POR_MILLA   FLOAT
);

-- ---------------------------------------------------------------------
-- 2. Comprobaciones (después de que cargar_snowflake termine en success)
-- ---------------------------------------------------------------------

-- 2.1 La tabla existe y tiene las 10 columnas, en este orden.
SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
FROM BIGDATA.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'DEV' AND TABLE_NAME = 'SINERGIA4_CONCENTRACION_LLUVIA'
ORDER BY ORDINAL_POSITION;

-- 2.2 Qué meses llegaron y cuántas filas y viajes trae cada uno
--     (deben ser los 12 meses de 2024).
SELECT TO_CHAR(FECHA, 'YYYY-MM') AS MES,
       COUNT(*)                  AS FILAS,
       SUM(VIAJES)               AS VIAJES,
       COUNT(DISTINCT FECHA)     AS DIAS
FROM BIGDATA.DEV.SINERGIA4_CONCENTRACION_LLUVIA
GROUP BY MES
ORDER BY MES;

-- 2.3 El grano se cumple: ninguna combinación fecha-zona-empresa repetida (debe dar 0 filas).
SELECT FECHA, PU_LOCATION_ID, EMPRESA, COUNT(*) AS VECES
FROM BIGDATA.DEV.SINERGIA4_CONCENTRACION_LLUVIA
GROUP BY FECHA, PU_LOCATION_ID, EMPRESA
HAVING COUNT(*) > 1;

-- 2.4 Cuántos días cae en cada nivel de lluvia (para confirmar los umbrales del contrato).
SELECT NIVEL_LLUVIA, COUNT(DISTINCT FECHA) AS DIAS
FROM BIGDATA.DEV.SINERGIA4_CONCENTRACION_LLUVIA
GROUP BY NIVEL_LLUVIA
ORDER BY DIAS DESC;
