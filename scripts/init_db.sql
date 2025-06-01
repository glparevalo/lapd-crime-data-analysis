/* 

============================================================================
						CREATE DATABASE AND SCHEMAS
============================================================================

Database:
	This script is used to create a database named "lapd_crime_db".
	The script checks first if the database exists. If so, the database 
	is dropped and recreated. 

Schemas:
	The schemas created are bronze, silver, and gold. 

*/

USE master;
GO

-- Drop then recreate the "lapd_crime_db" database

IF EXISTS (SELECT 1 FROM sys.databases where name = 'lapd_crime_database')
BEGIN
	ALTER DATABASE lapd_crime_database SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE lapd_crime_database;
END;
GO

-- Recreate/create the database 

CREATE DATABASE lapd_crime_database;
GO

-- Create schemas

USE lapd_crime_database;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
