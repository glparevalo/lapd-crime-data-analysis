/*
=============================================================================
			 DDL Gold: Create Gold Views
=============================================================================

Creating the Gold Views:
	This script is used to create views using the normalized silver 
	tables for the Gold later. This layer consists of the fact table 
	and dimension tables configured in a Star Schema.

	Each view has a transformation that allows for an understandable, 
	usable, and clean data for business users and analysts.

Example usage:
	Query "SELECT * FROM gold.dim_method" to see the method-related 
	view. The views can also be joined to other tables via the 
	fact table.

=============================================================================
*/

-- ============================================
--		DIMENSION: Method
-- ============================================

IF OBJECT_ID('gold.dim_method', 'V') IS NOT NULL
    DROP VIEW gold.dim_method;
GO

create view gold.dim_method as 
	select 
		sk_method_key as method_key,
		p.part_name as part,
		p.part_category as category,
		c.crime_cd_desc as crime_committed,
		w.weapon_desc as weapon_used
	from silver.dim_method m
	left join silver.sub_dim_part p
	on m.part = p.part

	left join silver.sub_dim_crime c
	on m.crime_cd = c.crime_cd

	left join silver.sub_dim_weapon w
	on m.weapon_used_cd = w.weapon_used_cd;

GO

-- ============================================
--		DIMENSION: Location
-- ============================================

IF OBJECT_ID('gold.dim_location', 'V') IS NOT NULL
    DROP VIEW gold.dim_location;
GO

create or alter view gold.dim_location as
	select
		m.sk_location_key as location_key,
		a.area_name as crime_area,
		p.premis_desc as crime_premise,
		ad.crime_address,
		ad.crime_lat as crime_latitude,
		round(cast(ad.crime_lat as decimal(8,5)), 2) as rounded_latitude,
		ad.crime_lon as crime_longitude,
		round(cast(ad.crime_lon as decimal(8,5)), 2) as rounded_longitude
	from silver.dim_location m

	left join silver.sub_dim_premis p
	on m.premis_cd = p.premis_cd

	left join silver.sub_dim_area a
	on m.area = a.area

	left join silver.sub_dim_address ad
	on m.sk_address_key = ad.sk_address_key;

GO

-- ============================================
--	    DIMENSION: Victim Profile
-- ============================================

IF OBJECT_ID('gold.dim_victim_profile', 'V') IS NOT NULL
    DROP VIEW gold.dim_victim_profile;
GO

create or alter view gold.dim_victim_profile as
	select 
		sk_vict_key as victim_key,
		vict_age as victim_age,
		CASE 
		WHEN vict_age IS NULL OR vict_age <= 0 THEN 'NO AGE DISCLOED'
		WHEN vict_age < 18 THEN '<18'
		WHEN vict_age BETWEEN 18 AND 24 THEN '18 - 24'
		WHEN vict_age BETWEEN 25 AND 34 THEN '25 - 34'
		WHEN vict_age BETWEEN 35 AND 44 THEN '35 - 44'
		WHEN vict_age BETWEEN 45 AND 54 THEN '45 - 54'
		WHEN vict_age BETWEEN 55 AND 64 THEN '55 - 64'
		ELSE '65+'
		END AS victim_age_category,
		vict_sex as victim_sex,
		UPPER(d.vict_descent_desc) as victim_descent
	from silver.dim_victim_profile m
	left join silver.sub_dim_descent d
	on m.vict_descent = d.vict_descent;

GO

-- ============================================
--		DIMENSION: Mo Code
-- ============================================

IF OBJECT_ID('gold.dim_mo_code', 'V') IS NOT NULL
    DROP VIEW gold.dim_mo_code;
	
GO

create view gold.dim_mo_code as 
	select
		dr_no,
		mo_code
	from silver.dim_mo_code;

GO

-- ============================================
--		FACT: Crime Event
-- ============================================

IF OBJECT_ID('gold.fact_crime_specifics', 'V') IS NOT NULL
    DROP VIEW gold.fact_crime_specifics;
GO

CREATE OR ALTER VIEW gold.fact_crime_specifics AS
	select 
		sk_report_key as report_key,
		dr_no,
		date_reported,
		t.date_occurred,
		t.time_occurred,
		CASE 
			WHEN time_occurred IS NULL THEN 'NO TIME DISCLOSED'
			WHEN CAST(time_occurred AS TIME) BETWEEN '00:00:00' AND '05:59:59' THEN 'LATE NIGHT'
			WHEN CAST(time_occurred AS TIME) BETWEEN '06:00:00' AND '11:59:59' THEN 'MORNING'
			WHEN CAST(time_occurred AS TIME) BETWEEN '12:00:00' AND '17:59:59' THEN 'AFTERNOON'
			WHEN CAST(time_occurred AS TIME) BETWEEN '18:00:00' AND '21:59:59' THEN 'EVENING'
			WHEN CAST(time_occurred AS TIME) BETWEEN '22:00:00' AND '23:59:59' THEN 'NIGHT'
			ELSE 'INVALID TIME'
		END AS time_category,
		report_district_no,
		s.status_desc as report_status,
		sk_method_key as method_key,
		sk_location_key as location_key,
		sk_vict_key as victim_key
	FROM silver.fact_specifics m
	left join silver.dim_time t
	on m.sk_time_key = t.sk_time_key

	left join silver.dim_status s
	on m.status_cd = s.status_cd;