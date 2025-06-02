/*
=============================================================================
					DDL Silver: Create the Silver Table
=============================================================================

Silver Flat Table:
	This script is used to create the silver table which consists of the  
	cleaned data of the LAPD Crime Dataset. This table is used to transform
	and normalize the dataset to make it business-ready. This script drops 
	the table if it already exists in the database then creates a new one.

3NF Tables:
	After normalizing, this script also creates the split tables.

=============================================================================
*/

-- Silver LAPD Crime Database
IF OBJECT_ID('silver.lapd_crime_database', 'U') IS NOT NULL
    DROP TABLE silver.lapd_crime_database;

GO

CREATE TABLE silver.lapd_crime_database (
    dr_no					INT,
    date_reported			DATE,
    date_occurred			DATE,
    time_occurred			TIME(0),
    area_id					INT,
    area_name				NVARCHAR(150),
    report_district_no		INT,
    part					INT,
    crime_cd				INT,
    crime_cd_desc			NVARCHAR(150),
            
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
    crime_lat				NVARCHAR(150),
    crime_lon				NVARCHAR(150)
);

GO

/*
==========================================================
						3NF Tables 
==========================================================

*/

-- =================== Crime Setting =====================

-- Crime Setting
IF OBJECT_ID('silver.crime_setting', 'U') IS NOT NULL
	DROP TABLE silver.crime_setting;
GO

CREATE TABLE silver.crime_setting (
	dr_no					INT PRIMARY KEY,
	date_reported			DATE,
	date_occurred			DATE,
	time_occurred			TIME(0), 
	area					INT,
	sk_location_key			INT
);

GO

-- Area Table
IF OBJECT_ID('silver.area_table', 'U') IS NOT NULL
	DROP TABLE silver.area_table;
GO

CREATE TABLE silver.area_table (
	area					INT,
	area_name				NVARCHAR(150),
);

GO

-- Location Table
IF OBJECT_ID('silver.location_table', 'U') IS NOT NULL
	DROP TABLE silver.location_table;
GO

CREATE TABLE silver.location_table (
        sk_location_key		INT	PRIMARY KEY,
        premis_cd			INT,
        crime_location		NVARCHAR(150),
        crime_lat			NVARCHAR(150),
        crime_lon			NVARCHAR(150)
);

GO

-- Premise Table
IF OBJECT_ID('silver.premis_table', 'U') IS NOT NULL
	DROP TABLE silver.premis_table;
GO

CREATE TABLE silver.premis_table (
	premis_cd				INT,
	premis_desc				NVARCHAR(150),
);

GO

-- =================== Crime Method =====================

IF OBJECT_ID('silver.crime_method', 'U') IS NOT NULL
	DROP TABLE silver.crime_method;
GO

create table silver.crime_method(
	sk_crime_method_key		int primary key,
	part 					int,
	crime_cd 				int,
	weapon_used_cd 			int
)

GO

IF OBJECT_ID('silver.crime_table', 'U') IS NOT NULL
	DROP TABLE silver.crime_table;
GO

CREATE TABLE silver.crime_table (
	crime_cd				INT,
	crime_desc				NVARCHAR(150)
);

GO


-- Create Weapon Table
IF OBJECT_ID('silver.weapon_table', 'U') IS NOT NULL
	DROP TABLE silver.weapon_table;
GO

CREATE TABLE silver.weapon_table (
	weapon_used_cd			INT,
	weapon_used_desc		NVARCHAR(150)
);

GO

-- Crime Victim Profile

IF OBJECT_ID('silver.crime_victim_profile', 'U') IS NOT NULL
	DROP TABLE silver.crime_victim_profile;
GO

CREATE TABLE silver.crime_victim_profile (
	sk_victim_profile_key	INT PRIMARY KEY,
	vict_age				INT,
	vict_sex				NVARCHAR(150),
	vict_descent			NVARCHAR(150),
);

GO

IF OBJECT_ID('silver.victim_table', 'U') IS NOT NULL
	DROP TABLE silver.victim_table;
GO

CREATE TABLE silver.victim_table (
	vict_descent			NVARCHAR(150),
	vict_descent_desc		NVARCHAR(150),
);

GO

-- Crime Case Status

IF OBJECT_ID('silver.crime_status', 'U') IS NOT NULL
	DROP TABLE silver.crime_status;
GO

create table silver.crime_status(
	sk_crime_status_key		INT PRIMARY KEY,
	report_district_no		INT,
	status_cd				NVARCHAR(150)
)

-- Create Status Table
IF OBJECT_ID('silver.status_table', 'U') IS NOT NULL
	DROP TABLE silver.status_table;
GO

create table silver.status_table(
	status_cd	NVARCHAR(150) PRIMARY KEY,
	status_desc	NVARCHAR(150)
)

