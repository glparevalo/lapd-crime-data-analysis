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

IF OBJECT_ID('bronze.lapd_crime_data', 'U') IS NOT NULL
	DROP TABLE bronze.lapd_crime_data;
GO

CREATE TABLE bronze.lapd_crime_data (
	dr_no					INT,
	date_reported			datetime,
	date_occurred			date,
	time_occurred			time,
	area					INT,
	area_name				NVARCHAR(200),
	report_district_no		INT,
	part					INT,
	crime_cd				INT,
	crime_cd_desc			NVARCHAR(200),
	mo_codes				NVARCHAR(200),
	vict_age				INT,
	vict_sex				NVARCHAR(200),
	vict_descent			NVARCHAR(200),
	premis_cd				INT,
	premis_desc				NVARCHAR(200),
	weapon_used_cd			INT,
	weapon_desc				NVARCHAR(200),
	status_cd				NVARCHAR(200),
	status_desc				NVARCHAR(200),
	crime_location			NVARCHAR(200),
	cross_street			NVARCHAR(200),
	crime_lat				DECIMAL(8,5),
	crime_lon				DECIMAL(8,5)
);
