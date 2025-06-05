-- location

INSERT INTO gold.dim_location(
	location_key,
	premise,
	crime_address,
	crime_address_latitude,
	crime_address_longitude
)

SELECT
    m.sk_location_key AS location_key,
    p.premis_desc AS premise,
    m.crime_location AS crime_address,
    CAST(m.crime_lat AS DECIMAL(8, 5)) AS crime_address_latitude,
    CAST(m.crime_lon AS DECIMAL(8, 5)) AS crime_address_longitude
FROM silver.location_table AS m
LEFT JOIN silver.premis_table AS p
    ON m.premis_cd = p.premis_cd;

-- victim profile

INSERT INTO gold.dim_victim_profile(
	victim_profile_key,
	victim_age,
	victim_sex,
	victim_descent
)

SELECT 
    m.sk_victim_profile_key AS victim_profile_key,
    m.vict_age AS victim_age,
    m.vict_sex AS victim_sex,
    v.vict_descent_desc AS victim_descent
FROM silver.crime_victim_profile AS m
LEFT JOIN silver.victim_table AS v
    ON m.vict_descent = v.vict_descent;

-- status
INSERT INTO gold.dim_status(
	status_key,
	reporting_district_number,
	status_description
)
SELECT 
    m.sk_crime_status_key AS status_key,
    m.report_district_no AS reporting_district_number,
    s.status_desc AS status_description
FROM silver.crime_status AS m
LEFT JOIN silver.status_table AS s
    ON m.status_cd = s.status_cd;

-- dim_method
INSERT INTO gold.dim_method(
	method_key,
	part,
	category,
	crime,
	weapon_used
)
sELECT
    m.sk_crime_method_key AS method_key,
    p.part_name AS part,
    p.category AS part_category,
    c.crime_desc AS crime,
    w.weapon_used_desc AS weapon_used
FROM silver.crime_method AS m
LEFT JOIN silver.crime_table AS c
    ON m.crime_cd = c.crime_cd
LEFT JOIN silver.weapon_table AS w
    ON m.weapon_used_cd = w.weapon_used_cd
LEFT JOIN silver.part_table AS p
    ON m.part = p.part;

-- fact crime event

INSERT INTO gold.fact_crime_event(
	crime_key,
	dr_no,
	date_reported,
	date_occurred,
	time_occurred,
	area,
	location_key,
	method_key,
	victim_profile_key,
	status_key
)


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