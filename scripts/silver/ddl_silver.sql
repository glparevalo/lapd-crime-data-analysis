/*
================================================================================
		    DDL Silver: Create the Silver Table
================================================================================

Silver Flat Table:
	This script is used to create the silver table which consists of the  
	cleaned data of the LAPD Crime Dataset. This table is used to transform
	and normalize the dataset to make it business-ready. This script drops 
	the table if it already exists in the database then creates a new one.

3NF Tables:
	After normalizing, this script also creates the split tables.

================================================================================
*/

-- Silver LAPD Crime Database
IF OBJECT_ID('silver.lapd_crime_data', 'U') IS NOT NULL
    DROP TABLE silver.lapd_crime_data;

GO

CREATE TABLE silver.lapd_crime_data (
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
    crime_lat				NVARCHAR(150),
    crime_lon				NVARCHAR(150)
);

GO

-- 1NF Silver

IF OBJECT_ID('silver.norm_lapd_crime_data', 'U') IS NOT NULL
    DROP TABLE silver.norm_lapd_crime_data;

GO

create table silver.norm_lapd_crime_data(
	dr_no					INT,
	date_reported			date,
	date_occurred			date,
	time_occurred			time(0),
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
	crime_lat				NVARCHAR(200),
	crime_lon				NVARCHAR(200)
)

/*
==========================================================
				3NF Tables 
==========================================================

*/

-- =================== Dim: Crime Location =====================

-- Dimension: Location
IF OBJECT_ID('silver.dim_location', 'U') IS NOT NULL
	DROP TABLE silver.dim_location;
GO

create table silver.dim_location(
	sk_location_key		INT,
	area				INT,
	premis_cd			INT,
	sk_address_key		INT
);

GO

-- Sub-dimension: Address
IF OBJECT_ID('silver.sub_dim_address', 'U') IS NOT NULL
	DROP TABLE silver.sub_dim_address;
GO

create table silver.sub_dim_address (
	sk_address_key		int,
	crime_address		nvarchar(200),
	crime_lat			nvarchar(200),
	crime_lon			nvarchar(200)
);

GO

-- Sub-dimension Area
IF OBJECT_ID('silver.sub_dim_area', 'U') IS NOT NULL
	DROP TABLE silver.sub_dim_area;
GO

create table silver.sub_dim_area(
	area		INT,
	area_name	NVARCHAR(200)
);

GO

-- Sub-dimension Area Premise
IF OBJECT_ID('silver.sub_dim_premis', 'U') IS NOT NULL
	DROP TABLE silver.sub_dim_premis;
GO

create table silver.sub_dim_premis(
	premis_cd		INT,
	premis_desc		NVARCHAR(200)
)

GO

-- =================== Dim: Crime Method =====================

-- Dimension: Method
IF OBJECT_ID('silver.dim_method', 'U') IS NOT NULL
	DROP TABLE silver.dim_method;
GO

create table silver.dim_method(
	sk_method_key		INT,
	part				INT,
	crime_cd			INT,
	weapon_used_cd		INT
);

GO

-- Sub-dimension: Part
IF OBJECT_ID('silver.sub_dim_part', 'U') IS NOT NULL
	DROP TABLE silver.sub_dim_part;
GO

create table silver.sub_dim_part(
	part			INT,
	part_name		NVARCHAR(200),
	part_category	NVARCHAR(200)
);

GO

-- Sub-dimension: Part
IF OBJECT_ID('silver.sub_dim_crime', 'U') IS NOT NULL
	DROP TABLE silver.sub_dim_crime;
GO

create table silver.sub_dim_crime(
	crime_cd		INT,
	CRIME_CD_DESC	nvarchar(200)
);

GO

-- Sub-dimension: Weapon
IF OBJECT_ID('silver.sub_dim_weapon', 'U') IS NOT NULL
	DROP TABLE silver.sub_dim_weapon;
GO

create table silver.sub_dim_weapon(
	weapon_used_cd		INT,
	weapon_desc			NVARCHAR(200)
);

GO

-- =================== Dim: Victim Profile =====================

-- Dimension: Victim Profile
IF OBJECT_ID('silver.dim_victim_profile', 'U') IS NOT NULL
	DROP TABLE silver.dim_victim_profile;
GO

create table silver.dim_victim_profile(
	sk_vict_key		INT,
	vict_age		INT,
	Vict_sex		NVARCHAR(200),
	vict_descent	NVARCHAR(200)
);

GO

-- Sub-dimension: Victim Descent
IF OBJECT_ID('silver.sub_dim_descent', 'U') IS NOT NULL
	DROP TABLE silver.sub_dim_descent;
GO

create table silver.sub_dim_descent(
	vict_descent		NVARCHAR(200),
	vict_descent_desc	NVARCHAR(200)
);

GO

-- =================== Dim: Status =====================
IF OBJECT_ID('silver.dim_status', 'U') IS NOT NULL
	DROP TABLE silver.dim_status;
GO

create table silver.dim_status(
	status_cd		nvarchar(200),
	status_desc		NVARCHAR(200)
);

GO

-- =================== Dim: Crime Time =====================
IF OBJECT_ID('silver.dim_time', 'U') IS NOT NULL
	DROP TABLE silver.dim_time;
GO

create table silver.dim_time(
	sk_time_key		INT,
	date_occurred	DATE,
	time_occurred	TIME(0)
)

GO

-- =================== Dim: MO Codes =====================
IF OBJECT_ID('silver.dim_mo_code', 'U') IS NOT NULL
	DROP TABLE silver.dim_mo_code;
GO

create table silver.dim_mo_code(
	dr_no			INT,
	mo_code			NVARCHAR(200)
)

-- =================== Fact: Specifics =====================

IF OBJECT_ID('silver.sub_dim_descent', 'U') IS NOT NULL
	DROP TABLE silver.sub_dim_descent;
GO

create table silver.fact_specifics(
	dr_no					INT,
	report_district_no		INT,
	date_reported			DATE,
	status_cd				NVARCHAR(200),
	sk_time_key				INT,
	sk_location_key			INT,
	sk_method_key			INT,
	sk_vict_key				INT
);

