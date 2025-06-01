/* 
=============================================================================
		      		DDL Bronze: Create the Bronze Table
=============================================================================

Bronze Table:
	This script is used to create the Bronze table which consists of the raw 
	data of the LAPD Crime Dataset. This table is used as a basis to
	create views that will help normalize the dataset. This script drops 
	the table if it already exists then creates a new one.

=============================================================================
*/

IF OBJECT_ID('bronze.lapd_crime_database', 'U') IS NOT NULL
	DROP TABLE bronze.lapd_crime_database;
GO

CREATE TABLE bronze.lapd_crime_database (
	dr_no					INT,
	date_reported			datetime,
	date_occurred			date,
	time_occurred			time,
	area					INT,
	area_name				NVARCHAR(50),
	report_district_no		INT,
	part					INT,
	crime_cd				INT,
	crime_cd_desc			NVARCHAR(100),
	mo_codes				NVARCHAR(50),
	vict_age				INT,
	vict_sex				NVARCHAR(50),
	vict_descent			NVARCHAR(50),
	premis_cd				INT,
	premis_desc				NVARCHAR(120),
	weapon_used_cd			INT,
	weapon_desc				NVARCHAR(120),
	status_cd				NVARCHAR(50),
	status_desc				NVARCHAR(120),
	crime_location			NVARCHAR(120),
	cross_street			NVARCHAR(120),
	crime_lat				FLOAT,
	crime_lon				FLOAT
);
