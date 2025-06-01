/*
=============================================================================
					Checks Bronze: Check the Bronze Table
=============================================================================

Bronze Table:
	This script is used to check the bronze table: what the initial data 
    quality looks like. It is also helpful for arranging a game plan and 
    learning immediate insights. 

=============================================================================
*/

SELECT 
	dr_no,
	date_reported,
	date_occurred,
	time_occurred,
	area,
	area_name,
	report_district_no,
	part,
	crime_cd,
	crime_cd_desc,
	mo_codes,
	vict_age,
	vict_sex,
	vict_descent,
	premis_cd,
	premis_desc,
	weapon_used_cd,
	weapon_desc,
	status_cd,
	status_desc,
	crime_location,
	cross_street,
	crime_lat,
	crime_lon
FROM bronze.lapd_crime_database

