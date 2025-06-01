/*
=============================================================================
					DDL Silver: Create the Silver Table
=============================================================================

Silver Flat Table:
	This script is used to create the silver table which consists of the  
	cleaned data of the LAPD Crime Dataset. This table is used to transform
	and normalize the dataset to make it business-ready. This script drops 
	the table if it already exists in the database then creates a new one.

=============================================================================
*/

IF OBJECT_ID('silver.lapd_crime_database', 'U') IS NOT NULL
	DROP TABLE silver.lapd_crime_database;
GO

CREATE TABLE silver.lapd_crime_database (
	dr_no					INT,
	date_reported			DATE,
	date_occurred			DATE,
	time_occurred			TIME,
	area					INT,
	area_name				NVARCHAR(150),
	report_district_no		INT,
	part					INT,
	crime_cd				INT,
	crime_cd_desc			NVARCHAR(150),
	
	mo_codes				NVARCHAR(150),
	vict_age				INT,
	vict_sex				NVARCHAR(150),
	vict_descent			NVARCHAR(150),
	premis_cd				INT,
	premis_desc				NVARCHAR(150),
	weapon_used_cd			INT,
	weapon_desc				NVARCHAR(150),
	status_cd				NVARCHAR(150),
	status_desc				NVARCHAR(150),
	
	crime_location			NVARCHAR(150),
	cross_street			NVARCHAR(150),
	crime_lat				FLOAT,
	crime_lon				FLOAT
);