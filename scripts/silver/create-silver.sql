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