/*
CREATE DATABASES AND SCHEMAS 
Everytime you make a script it's a good practice to make some general comments about what was this script about, it's
going to be useful for you and your teamates. 

Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
within the database: 'bronze', 'silver', and 'gold'.
WARNING: 
Running this script will drop the entire 'DataWarehouse database if it exists.
All data in the database will be permanently deleted. Proceed with caution
and ensure you have proper backups before running this script.
*/

-- USE Master to create the database
USE Master
GO 

-- Drop and Recreate the DataWarehouse if it exists 
IF EXISTS (SELECT 1 FROM sys.database WHERE name = 'WareHouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the database DataWarehouse
CREATE DATABASE DataWarehouse
GO

--Use it
USE DataWarehouse;
GO

--Create the schemas for each layer of your database 
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver; 
GO
CREATE SCHEMA gold;
GO

