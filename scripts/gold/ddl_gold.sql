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

CREATE TABLE gold.fact_crime_event(
	crime_key			INT,
	dr_no				INT,
	date_reported		DATE,
	date_occurred		DATE,
	time_occurred		TIME(0),
	area				NVARCHAR(150),
	location_key		INT,
	method_key			INT,
	victim_profile_key	INT,
	status_key			INT
)
GO
