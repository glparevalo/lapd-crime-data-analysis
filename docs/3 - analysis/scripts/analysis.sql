/* Kept for visualizing dataset
select
	top 100 
	*
from gold.fact_crime_event
*/

-- number of crimes per date (year and month)
select
	year(date_occurred) as year,
	month(date_occurred) as month,
	count(crime_key) as number_of_crimes
from gold.fact_crime_event
group by year(date_occurred), month(date_occurred)
order by year, month

-- number of crimes by area
select
	year(date_occurred),
	area,
	count(crime_key) as number_of_crimes
from gold.fact_crime_event
group by year(date_occurred), area
order by area

-- number of crimes by premise by year
select
	year(m.date_occurred) as year,
	l.premise,
	count(crime_key) as number_of_crimes
from gold.fact_crime_event as m
left join gold.dim_location l
on m.location_key = l.location_key
group by year(date_occurred), l.premise
order by year, number_of_crimes desc

-- number of weapons used per crime
select
	year(date_occurred) as year,
	m.crime,
	m.weapon_used,
	count(f.crime_key) as number_of_crimes
from gold.fact_crime_event as f
left join gold.dim_method as m
on f.method_key = m.method_key
group by year(date_occurred), m.crime, m.weapon_used
order by crime, number_of_crimes desc

-- number of crimes per part per year
select 
	year(f.date_occurred) as year,
	m.part,
	m.category,
	count(crime_key) as number_of_crimes
from gold.fact_crime_event as f
left join gold.dim_method as m
on f.method_key = m.method_key
group by year(f.date_occurred),m.category, m.part
order by year desc, part

-- number of crimes per victim age group
select 
	* 
from
	(select 
			year,
			age_group,
			crime,
			count(crime_key) as number_of_crimes,
			dense_rank() over (partition by year, age_group order by count(crime_key) desc) as ranking
		from
			(select
				year(date_occurred) as year,
				me.crime,
				case
					-- 0 means no age (victim is an object e.g. car)
					when victim_age < 18 and victim_age <> 0 then 'YOUTH' 
					when victim_age >= 18 and victim_age < 36 then 'YOUNG ADULT'
					when victim_age >= 36 and victim_age <= 60 then 'ADULT' 
					when victim_age > 60  then 'SENIOR'
				end as age_group,
				m.crime_key as crime_key
			from gold.fact_crime_event as m
			left join gold.dim_victim_profile as v
			on m.victim_profile_key = v.victim_profile_key
	
			left join gold.dim_method as me
			on m.method_key = me.method_key) s
		group by year, age_group, crime) p
where ranking <= 10 and age_group is not null
order by year desc, age_group, crime

-- number of crimes per victim sex
select
	* 
from 
	(select
		victim_sex,
		crime,
		count(crime_key) as number_of_crimes,
		DENSE_RANK() over (partition by victim_sex order by count(crime_key) desc) as ranking
	from 
		(select 
			victim_sex,
			me.crime,
			m.crime_key
		from gold.fact_crime_event m
		left join gold.dim_method me
		on m.method_key = me.method_key

		left join gold.dim_victim_profile v
		on m.victim_profile_key = v.victim_profile_key) a
	group by victim_sex, crime) s
where ranking <= 10 and (UPPER(victim_sex) = 'MALE' or upper(victim_sex) = 'FEMALE')
order by victim_sex

-- crimes inflicted upon victim descent per area
select
	year(date_occurred) as year,
	area,
	victim_descent,
	count(m.crime_key) as number_of_crimes
from gold.fact_crime_event m
left join gold.dim_method me
on m.method_key = me.method_key

left join gold.dim_victim_profile v
on m.victim_profile_key = v.victim_profile_key
group by area, victim_descent
order by area, count(m.crime_key) desc

-- crimes per location
select
	year,
	area,
	avg(crime_address_latitude) as average_latitude,
	avg(crime_address_longitude) as average_longitude,
	count(dr_no) as number_of_crimes
from
	(select 
		year(date_occurred) as year,
		m.area,
		crime_address_latitude, 
		crime_address_longitude,
		dr_no
	from gold.fact_crime_event m
	left join gold.dim_location l
	on m.location_key = l.location_key
	where crime_address_latitude <> 0 and crime_address_longitude <> 0) a
group by year, area
order by year, number_of_crimes desc

-- top addresses with most crimes
select 
	*
from 
(select
	year(date_occurred) as year,
	l.crime_address,
	count(crime_key) as number_of_crimes,
	DENSE_RANK() over (partition by year(date_occurred) order by count(crime_key) desc) as ranking
from gold.fact_crime_event m
left join gold.dim_location l
on m.location_key = l.location_key
group by year(date_occurred), l.crime_address) s
where ranking < 11
order by year, number_of_crimes desc

-- status 
select distinct status_description from gold.dim_status