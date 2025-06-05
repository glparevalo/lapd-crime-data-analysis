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
	area,
	count(crime_key) as number_of_crimes
from gold.fact_crime_event
group by area
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
	m.crime,
	m.weapon_used,
	count(f.crime_key) as number_of_crimes
from gold.fact_crime_event as f
left join gold.dim_method as m
on f.method_key = m.method_key
group by m.crime, m.weapon_used
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
* from gold.fact_crime_event