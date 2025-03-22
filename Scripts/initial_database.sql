-- ===========================================
-- Script to set up initial database and schemas
-- Project: Medallion Data Warehouse
-- Description:
--   - Creates the main Data Warehouse database
--   - Defines three schemas for multi-layered architecture:
--        1. bronze - for raw/ingested data
--        2. silver - for cleaned and transformed data
--        3. gold   - for aggregated and business-ready data
-- ===========================================

-- Create the main database
CREATE DATABASE DataWarehouseMed;

-- Create schema for raw ingested data (bronze layer)
CREATE SCHEMA bronze;

-- Create schema for cleaned and transformed data (silver layer)
CREATE SCHEMA silver;

-- Create schema for aggregated, business-consumable data (gold layer)
CREATE SCHEMA gold;
