-- method
create view gold.dim_method as

select
	m.sk_crime_method_key as method_key,
	p.part_name as part,
	p.category,
	c.crime_desc as crime,
	w.weapon_used_desc as weapon_used
from silver.crime_method as m
left join silver.crime_table as c
on m.crime_cd = c.crime_cd

left join silver.weapon_table as w
on m.weapon_used_cd = w.weapon_used_cd

left join silver.part_table p
on m.part = p.part


select
	m.sk_location_key as location_key,
	p.premis_desc as premise,
	m.crime_location,
	m.crime_lat,
	m.crime_lon,
	
from silver.location_table as m
left join silver.premis_table as p
on m.premis_cd = p.premis_cd

-- status
create view gold.dim_status as

select 
	m.sk_crime_status_key as status_key,
	m.report_district_no as reporting_district_number,
	s.status_desc as status
from silver.crime_status m
left join silver.status_table s
on m.status_cd = s.status_cd

select * from gold.dim_status

-- location

create view gold.dim_location as

select
	m.sk_location_key as location_key,
	p.premis_desc as premise,
	m.crime_location as crime_address,
	cast(m.crime_lat as decimal(8,5)) as crime_address_latitude,
	cast(m.crime_lon as decimal(8,5)) as crime_address_longitude
from silver.location_table as m
left join silver.premis_table as p
on m.premis_cd = p.premis_cd

-- victim profile

create view gold.dim_victim_profile as 

select 
	m.sk_victim_profile_key as victim_profile_key,
	m.vict_age as victim_age,
	m.vict_sex as victim_sex,
	v.vict_descent_desc as victim_descent
from silver.crime_victim_profile m
left join silver.victim_table v
on m.vict_descent = v.vict_descent

-- fact crime event

create view gold.fact_crime_event as 

select
	row_number() over (order by dr_no, date_reported) as crime_key,
	m.dr_no,
	m.date_reported,
	m.date_occurred,
	m.time_occurred,
	a.area_name as area,
	m.sk_location_key as location_key,
	m.sk_crime_method_key as method_key,
	m.sk_victim_profile_key as victim_profile_key,
	m.sk_crime_status_key as status_key
from silver.crime_setting m
left join silver.area_table a
on m.area_id = a.area_id

