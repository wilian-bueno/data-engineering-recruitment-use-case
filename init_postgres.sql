-- Airflow metadata database
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Beverage analytics serving database
CREATE USER beverage WITH PASSWORD 'beverage';
CREATE DATABASE beverage OWNER beverage;

\connect beverage beverage

-- Raw data exploration (profiling before transformation)
CREATE SCHEMA IF NOT EXISTS staging;

-- Medallion layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
