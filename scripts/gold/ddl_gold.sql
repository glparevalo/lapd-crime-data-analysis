/*
=============================================================================
						 DDL Gold: Create Gold Views
=============================================================================

Creating the Gold Views:
	This script is used to create views using the normalized silver tables 
	for the Gold later. This layer consists of the fact table and dimension
	tables configured in a Star Schema.

	Each view has a transformation that allows for an understandable, usable,
	and clean data for business users and analysts.

Example usage:
	Query "SELECT * FROM gold.dim_method" to see the method-related view.
	The views can also be joined to other tables via the fact table.

=============================================================================
*/


-- ============================================
--				DIMENSION: Method
-- ============================================

IF OBJECT_ID('gold.dim_method', 'V') IS NOT NULL
    DROP VIEW gold.dim_method;
GO

CREATE TABLE gold.dim_method(
	method_key				INT,
	part					NVARCHAR(150),
	category				NVARCHAR(150),
	crime					NVARCHAR(150),
	weapon_used				NVARCHAR(150)
);
GO

-- ============================================
--				DIMENSION: Status
-- ============================================

IF OBJECT_ID('gold.dim_status', 'V') IS NOT NULL
    DROP VIEW gold.dim_status;
GO

CREATE TABLE gold.dim_status(
	status_key				        INT,
	reporting_district_number	    INT,
	status_description		        NVARCHAR(150)
)
GO

-- ============================================
--				DIMENSION: Location
-- ============================================

IF OBJECT_ID('gold.dim_location', 'V') IS NOT NULL
    DROP VIEW gold.dim_location;
GO

CREATE TABLE gold.dim_location(
	location_key	            INT,
	premise			            NVARCHAR(150),
	crime_address	            NVARCHAR(150),
	crime_address_latitude		DECIMAL(8,5),
	crime_address_longitude		DECIMAL(8,5)
)
GO

-- ============================================
--			DIMENSION: Victim Profile
-- ============================================

IF OBJECT_ID('gold.dim_victim_profile', 'V') IS NOT NULL
    DROP VIEW gold.dim_victim_profile;
GO

CREATE TABLE gold.dim_victim_profile(
	victim_profile_key		INT,
	victim_age				INT,
	victim_sex				NVARCHAR(50),
	victim_descent			NVARCHAR(150)
)
GO

-- ============================================
--				FACT: Crime Event
-- ============================================

IF OBJECT_ID('gold.fact_crime_event', 'V') IS NOT NULL
    DROP VIEW gold.fact_crime_event;
GO

CREATE VIEW gold.fact_crime_event AS
SELECT
    ROW_NUMBER() OVER (ORDER BY m.dr_no, m.date_reported) AS crime_key,
    m.dr_no,
    m.date_reported,
    m.date_occurred,
    m.time_occurred,
    a.area_name AS area,
    m.sk_location_key AS location_key,
    m.sk_crime_method_key AS method_key,
    m.sk_victim_profile_key AS victim_profile_key,
    m.sk_crime_status_key AS status_key
FROM silver.crime_setting AS m
LEFT JOIN silver.area_table AS a
    ON m.area_id = a.area_id;
GO
