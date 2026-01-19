-- ============================================================
-- Create Database and Schemas
-- ============================================================
-- Script Purpose:
--     This script creates a new database named 'datawarehouse' after checking if it already exists.
--     If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
--     within the database: 'bronze', 'silver', and 'gold'.
--
-- WARNING:
--     Running this script will drop the entire 'datawarehouse' database if it exists.
--     All data in the database will be permanently deleted. Proceed with caution
--     and ensure you have proper backups before running this script.
-- ============================================================

-- Drop the database if it exists and recreate it
\c postgres  -- Connect to default database to drop the target database
DROP DATABASE IF EXISTS datawarehouse;

-- Create the database
CREATE DATABASE datawarehouse;

-- Connect to the new database
\c datawarehouse

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
