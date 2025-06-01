SELECT * FROM bronze.lapd_crime_database

/*
 Tests scripts

*/

-- 1) check for duplicate dr_no (primary key)

select dr_no, count(*) 
	from (select * from bronze.lapd_crime_database) t 
	group by dr_no
		having count(*) > 1

-- Findings: no duplicate dr_no 

-- 2) Must: Date reported > date_occurred 

SELECT dr_no, date_reported 
from bronze.lapd_crime_database
where date_reported < date_occurred

-- Findings: No instance of Date reported < date_occurred  

-- 3) age (there should be no negative victim age, ages above 100, 0 age)

SELECT dr_no, vict_age,
	case 
		when vict_age < 0 then abs(vict_age)
		when vict_age = 0 then null
		else vict_age 
	end as new_vict_age
from bronze.lapd_crime_database
where vict_age < 0 OR vict_age >= 100 or vict_age = 0
 
/* 

Findings: 
	- there are instances of ages above 95 and may be the real case.
	- there are negative ages, which are not found to be age codes/placeholders
	  hence they are changed to positive.
	- there may be no victims present when the crimes are committed but there are
	  no facts to support that. Therefore, ages with 0 values are replaced with nulls
	  to allow analysts to make objective decisions.
*/

-- 4) victim sex

SELECT distinct vict_sex,
case
	when lower(trim(vict_sex)) = 'h' then 'Other'
	when lower(trim(vict_sex)) = '-' then 'N/A'
	when lower(trim(vict_sex)) is null then 'N/A'
	when lower(trim(vict_sex)) = 'x' then 'N/A'
	else vict_sex
end as new_vict_sex
from bronze.lapd_crime_database
where vict_sex = 'H' or vict_sex = '-'

/* 

Findings: 
	- "H" is replaced by "Other" despite having only "F", "M", and "X" defined 
	  in the data catalog since "H" could be assigned for inclusivity efforts
	- "X" and "-" are replaced with "N/A"

*/

-- 4) premis_cd: which is usually a 3-digit number

SELECT distinct premis_cd

from bronze.lapd_crime_database
where premis_cd < 100 or premis_cd > 1000

	-- Findings: no changes required

-- 5) weapon used cd: check for outliers (also a 3 digit number)

SELECT distinct len(weapon_used_cd) as len_wuc

from bronze.lapd_crime_database
where len(weapon_used_cd) <> 3 

-- 6) weapon desc: check for invalid data (desc not given if code is given)

SELECT distinct weapon_used_cd, weapon_desc

from bronze.lapd_crime_database
where weapon_used_cd is not null and weapon_desc is null

-- all is good, check also for vice versa (e.g. weapon used is null but there is desc)


-- 7) status code and status desc: check if theres a mismatch
SELECT distinct 
	status_cd, 
	status_desc,
	case
		when status_cd = 'CC' and status_desc = 'UNK' then 'Case Closed'
		when status_cd is null and status_desc = 'UNK' then 'Unknown'
		when status_cd = 'JA' then 'Juvenile Arrest'
		when status_cd = 'JO' then 'Juvenile Other'
		when status_cd = 'AO' then 'Adult Other'
		when status_cd = 'AA' then 'Adult Arrest'
		when status_cd = 'IC' then 'Investigation Continued'
	end as new_status_desc

from bronze.lapd_crime_database

/* 

Findings: 
	- "H" is replaced by "Other" despite having only "F", "M", and "X" defined 
	  in the data catalog since "H" could be assigned for inclusivity efforts
	- "X" and "-" are replaced with "N/A"

*/

-- 8) crime location: address has many white spaces in between

SELECT distinct crime_location,
	replace(replace(replace(
		replace(replace(replace(
			replace(Replace(REPLACE(crime_location, '  ',' ')
			, '  ', ' '), '  ', ' '), '  ', ' '),
		'  ', ' '), '  ', ' '), '  ', ' '),
	'  ', ' '), '  ', ' ') as new_crime_loc
from bronze.lapd_crime_database 


-- 9) vict_descent must follow data catalog

SELECT distinct vict_descent,
case	
	when vict_descent is null or vict_descent = '-' then 'X'
	else vict_descent
end as new_vict_descent

from bronze.lapd_crime_database 
order by vict_descent

-- Findings: nulls and "-" are replaced with "X" which stands for Unknown

-- crime cd desc

SELECT distinct crime_cd_desc,
replace(replace(crime_cd_desc, ';', ': '), '  ',' ') as new
from bronze.lapd_crime_database

-- PREMIS_DESC

SELECT DISTINCT CASE 
		WHEN CAST(premis_cd AS NVARCHAR(10)) is null then 'N/A'
		ELSE CAST(premis_cd AS NVARCHAR(10))
	END AS premis_cd,
	UPPER(trim(premis_desc)) AS premis_desc,
CASE 
	when REPLACE(premis_desc, '*', '') IS NULL THEN 'Unknown / Tentative'
	ELSE REPLACE(premis_desc, '*', '')
END AS new_premis_cd
FROM bronze.lapd_crime_database
ORDER BY premis_cd

