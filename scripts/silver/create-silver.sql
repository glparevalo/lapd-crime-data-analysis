/*
===================================================
				crime location
===================================================
*/

-- silver.dim_location

create table silver.dim_location(
	sk_location_key		INT,
	area				INT,
	premis_cd			INT,
	sk_address_key		INT
)

INSERT INTO silver.dim_location(
	sk_location_key,
	area,
	premis_cd,
	sk_address_key
)

select 
	ROW_NUMBER() over (order by area, premis_cd) as sk_location_key,
	area,
	premis_cd,
	sk_address_key
FROM 
(select distinct
	m.area,
	m.premis_cd,
	m.crime_location as crime_address,
	m.crime_lat,
	m.crime_lon,
	d.sk_address_key
from silver.norm_lapd_crime_data m
left join silver.sub_dim_address d
ON m.crime_location = d.crime_address
AND m.crime_lat = d.crime_lat
AND M.crime_lon = D.crime_lon) a

-- silver.sub_dim_address

create table silver.sub_dim_address (
	sk_address_key		int,
	crime_address		nvarchar(200),
	crime_lat			nvarchar(200),
	crime_lon			nvarchar(200)
)

insert into silver.sub_dim_address(
	sk_address_key		,
	crime_address		,
	crime_lat			,
	crime_lon
)

select
	row_number() over (order by crime_lat) as sk_address_key,
	crime_address,
	crime_lat,
	crime_lon
FROM(
select distinct
	crime_location as crime_address,
	crime_lat,
	crime_lon
from silver.norm_lapd_crime_data) a
order by sk_address_key

-- area table
create table silver.sub_dim_area(
	area		INT,
	area_name	NVARCHAR(200)
)

INSERT INTO silver.sub_dim_area(
	area,
	area_name
)
select distinct
	area,
	area_name
from silver.norm_lapd_crime_data

-- premise table
create table silver.sub_dim_premis(
	premis_cd		INT,
	premis_desc		NVARCHAR(200)
)

INSERT INTO silver.sub_dim_premis(
	PREMIS_CD,
	premis_desc
)
select distinct
	premis_cd,
	case 
		when premis_desc = 'N/A' THEN 'NO PREMISE PROVIDED'
		ELSE premis_desc
	END as premis_desc
from silver.norm_lapd_crime_data
order by premis_cd


/*
===================================================
				crime methods
===================================================
*/

create table silver.dim_method(
	sk_method_key		INT,
	part				INT,
	crime_cd			INT,
	mo_code				NVARCHAR(200),
	weapon_used_cd		INT
)

INSERT INTO silver.dim_method(
sk_method_key,
part,
crime_cd,
weapon_used_cd
)
select
	row_number() over (order by part, crime_cd) as sk_method_key,
	part,
	crime_cd,
	weapon_used_cd
FROM
(
select distinct
	part,
	crime_cd,
	-- crime_cd_desc, / no longer needed
	weapon_used_cd
from silver.norm_lapd_crime_data) a

-- crime part

create table silver.sub_dim_part(
	part	INT,
	part_name	NVARCHAR(200),
	part_category	NVARCHAR(200)
)

INSERT INTO silver.sub_dim_part(
part,
part_name,
part_category
)

select distinct
	part,
    CASE	
		WHEN part = 1 THEN 'Part I Crimes'
		WHEN part = 2 THEN 'Part II Crimes'
		ELSE 'N/A'
    END AS part_name,
    CASE	
		WHEN part = 1 THEN 'Serious Index Crime'
		WHEN part = 2 THEN 'Other / Non-Index Crime'
		ELSE 'Uncategorized' 
	END AS part_category
from silver.norm_lapd_crime_data

-- crime code
create table silver.sub_dim_crime(
	crime_cd		INT,
	CRIME_CD_DESC	nvarchar(200)
)

insert into silver.sub_dim_crime(
	crime_cd,
	crime_cd_desc
)

select distinct
	crime_cd,
	crime_cd_desc
from silver.norm_lapd_crime_data
order by crime_cd

-- weapon
create table silver.sub_dim_weapon(
	weapon_used_cd			INT,
	weapon_desc		NVARCHAR(200)
)

INSERT INTO silver.sub_dim_weapon(
	weapon_used_cd,
	weapon_desc
)
select distinct
	weapon_used_cd,
	weapon_desc
from silver.norm_lapd_crime_data
order by weapon_used_cd  


/*
===================================================
				crime victim profile
===================================================
*/

-- vcitim profile
create table silver.dim_victim_profile(
	sk_vict_key		INT,
	vict_age		INT,
	Vict_sex		NVARCHAR(200),
	vict_descent	NVARCHAR(200)
)

INSERT INTO silver.dim_victim_profile(
sk_vict_key,
vict_age,
Vict_sex,
vict_descent
)

select
	row_number() over (order by vict_age) as sk_vict_key,
	vict_age,
	vict_sex,
	vict_descent
FROM
(select distinct
	vict_age,
	vict_sex,
	vict_descent
from silver.norm_lapd_crime_data) a

-- victim descent

create table silver.sub_dim_descent(
	vict_descent		NVARCHAR(200),
	vict_descent_desc	NVARCHAR(200)
)

INSERT INTO silver.sub_dim_descent(
vict_descent,
vict_descent_desc
)

-- descent
select distinct
	vict_descent,
	CASE UPPER(TRIM(vict_descent))
		WHEN 'A' THEN 'Other Asian'
		WHEN 'B' THEN 'Black'
		WHEN 'C' THEN 'Chinese'
		WHEN 'D' THEN 'Cambodian'
		WHEN 'F' THEN 'Filipino'
		WHEN 'G' THEN 'Guamanian'
		WHEN 'H' THEN 'Hispanic/Latin/Mexican'
		WHEN 'I' THEN 'American Indian/Alaskan Native'
		WHEN 'J' THEN 'Japanese'
		WHEN 'K' THEN 'Korean'
		WHEN 'L' THEN 'Laotian'
		WHEN 'O' THEN 'Other'
		WHEN 'P' THEN 'Pacific Islander'
		WHEN 'S' THEN 'Samoan'
		WHEN 'U' THEN 'Hawaiian'
		WHEN 'V' THEN 'Vietnamese'
		WHEN 'W' THEN 'White'
		WHEN 'X' THEN 'Unknown'
		WHEN 'Z' THEN 'Asian Indian'
		ELSE 'Uncategorized'
	END AS vict_descent_desc
from silver.norm_lapd_crime_data

/*
===================================================
				fact crime specifics
===================================================
*/

create table silver.fact_specifics(
	dr_no					INT,
	report_district_no		INT,
	date_reported			DATE,
	status_cd				NVARCHAR(200),
	sk_time_key				INT,
	sk_location_key			INT,
	sk_method_key			INT,
	sk_vict_key				INT
)

with cte as(
select 
	l.sk_location_key,
	l.area,
	l.premis_cd,
	l.sk_address_key,
	a.crime_address,
	a.crime_lat,
	a.crime_lon
from silver.dim_location l 
left join silver.sub_dim_address a
ON l.SK_ADDRESS_key = a.sk_address_key)

INSERT INTO silver.fact_specifics(
dr_no,
report_district_no,
date_reported,
status_cd,
sk_time_key,
sk_location_key,
sk_method_key,
sk_vict_key
)

select distinct
	dr_no,
	report_district_no,
	date_reported,
	status_cd,
	t.sk_time_key,
	a.sk_location_key,
	d.sk_method_key,
	v.sk_vict_key
FROM silver.norm_lapd_crime_data m
LEFT JOIN silver.dim_time t
ON m.date_occurred = t.date_occurred
AND	m.time_occurred = t.time_occurred
-- where t.sk_time_key is null

left join cte a
on m.area = a.area
and m.premis_cd = a.premis_cd
and m.crime_location = a.crime_address
and m.crime_lat = a.crime_lat
and m.crime_lon = a.crime_lon


left join silver.dim_method d
ON m.part = d.part
AND m.crime_cd = d.crime_cd
AND m.weapon_used_cd = d.weapon_used_cd


left join silver.dim_victim_profile v
on m.vict_age = v.vict_age
and m.vict_sex =v.vict_sex
and m.vict_descent = v.vict_descent

select * from SILVER.fact_specifics

-- status
create table silver.dim_status(
	status_cd		nvarchar(200),
	status_desc		NVARCHAR(200)
)

INSERT INTO silver.dim_status(
	status_cd,
	status_desc
)
select distinct
	status_cd,
	case
		when status_cd = 'CC' then 'UNKNOWN'
		ELSE UPPER(TRIM(STATUS_DESC))
	END AS status_desc
from silver.norm_lapd_crime_data


-- crime time
create table silver.dim_time(
	sk_time_key		INT,
	date_occurred	DATE,
	time_occurred	TIME(0)
)

insert into silver.dim_time(
	sk_time_key		,
	date_occurred	,
	time_occurred	
)

select
	ROW_NUMBER() over (order by date_occurred, time_occurred) as sk_time_key,
	date_occurred,
	time_occurred
FROM 
(select distinct
	date_occurred,
	time_occurred
from silver.norm_lapd_crime_data) a
order by sk_time_key

-- dim mo code

-- crime specifics
create table silver.dim_mo_code(
	dr_no			INT,
	mo_code			NVARCHAR(200)
)

INSERT INTO silver.dim_mo_code(
	dr_no,
	mo_code
)
select distinct
	dr_no,
	case
		when mo_codes = 'No MO Codes' then 'NO MO CODE DISCLOSED'
		else mo_codes
	end as mo_code
from silver.norm_lapd_crime_data