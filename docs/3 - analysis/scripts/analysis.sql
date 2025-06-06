-- ============================================
--		Crimes per Year and Month
-- ============================================
IF OBJECT_ID('views.vw_crimes_per_date', 'V') IS NOT NULL
    DROP VIEW views.vw_crimes_per_date;
GO

CREATE VIEW views.vw_crimes_per_date AS
SELECT
    YEAR(date_occurred) AS year,
    MONTH(date_occurred) AS month,
    COUNT(crime_key) AS number_of_crimes
FROM gold.fact_crime_event
GROUP BY YEAR(date_occurred), MONTH(date_occurred);
GO

-- ============================================
--				Crimes by Area
-- ============================================
IF OBJECT_ID('views.vw_crimes_by_area', 'V') IS NOT NULL
    DROP VIEW views.vw_crimes_by_area;
GO

CREATE VIEW views.vw_crimes_by_area AS
SELECT
    YEAR(date_occurred) AS year,
    area,
    COUNT(crime_key) AS number_of_crimes
FROM gold.fact_crime_event
GROUP BY YEAR(date_occurred), area;
GO

-- ============================================
-- Crimes by Premise
-- ============================================
IF OBJECT_ID('views.vw_crimes_by_premise', 'V') IS NOT NULL
    DROP VIEW views.vw_crimes_by_premise;
GO

CREATE VIEW views.vw_crimes_by_premise AS
SELECT
    YEAR(m.date_occurred) AS year,
    l.premise,
    COUNT(m.crime_key) AS number_of_crimes
FROM gold.fact_crime_event AS m
LEFT JOIN gold.dim_location l ON m.location_key = l.location_key
GROUP BY YEAR(m.date_occurred), l.premise;
GO

-- ============================================
--			Weapons Used Per Crime
-- ============================================
IF OBJECT_ID('views.vw_weapons_used_per_crime', 'V') IS NOT NULL
    DROP VIEW views.vw_weapons_used_per_crime;
GO

CREATE VIEW views.vw_weapons_used_per_crime AS
SELECT
    YEAR(f.date_occurred) AS year,
    m.crime,
    m.weapon_used,
    COUNT(f.crime_key) AS number_of_crimes
FROM gold.fact_crime_event AS f
LEFT JOIN gold.dim_method AS m ON f.method_key = m.method_key
GROUP BY YEAR(f.date_occurred), m.crime, m.weapon_used;
GO

-- ============================================
--		Crimes per Part per Year
-- ============================================
IF OBJECT_ID('views.vw_crimes_per_part', 'V') IS NOT NULL
    DROP VIEW views.vw_crimes_per_part;
GO

CREATE VIEW views.vw_crimes_per_part AS
SELECT 
    YEAR(f.date_occurred) AS year,
    m.part,
    m.category,
    COUNT(f.crime_key) AS number_of_crimes
FROM gold.fact_crime_event AS f
LEFT JOIN gold.dim_method AS m ON f.method_key = m.method_key
GROUP BY YEAR(f.date_occurred), m.part, m.category;
GO

-- ============================================
--		Crimes by Victim Age Group
-- ============================================
IF OBJECT_ID('views.vw_crimes_by_victim_age', 'V') IS NOT NULL
    DROP VIEW views.vw_crimes_by_victim_age;
GO

CREATE VIEW views.vw_crimes_by_victim_age_group AS
SELECT 
    *
FROM (
    SELECT 
        year,
        age_group,
        crime,
        COUNT(crime_key) AS number_of_crimes,
        DENSE_RANK() OVER (PARTITION BY year, age_group ORDER BY COUNT(crime_key) DESC) AS ranking
    FROM (
        SELECT
            YEAR(f.date_occurred) AS year,
            m.crime,
            CASE
                WHEN v.victim_age < 18 AND v.victim_age <> 0 THEN 'YOUTH'
                WHEN v.victim_age BETWEEN 18 AND 35 THEN 'YOUNG ADULT'
                WHEN v.victim_age BETWEEN 36 AND 60 THEN 'ADULT'
                WHEN v.victim_age > 60 THEN 'SENIOR'
            END AS age_group,
            f.crime_key
        FROM gold.fact_crime_event f
        LEFT JOIN gold.dim_victim_profile v ON f.victim_profile_key = v.victim_profile_key
        LEFT JOIN gold.dim_method m ON f.method_key = m.method_key
    ) sub
    WHERE age_group IS NOT NULL
    GROUP BY year, age_group, crime
) final
WHERE ranking <= 10;
GO

-- ============================================
--			Crimes by Victim Sex
-- ============================================
IF OBJECT_ID('views.vw_crimes_by_victim_sex', 'V') IS NOT NULL
    DROP VIEW views.vw_crimes_by_victim_sex;
GO

CREATE VIEW views.vw_crimes_by_victim_sex AS
SELECT 
    *
FROM (
    SELECT 
        v.victim_sex,
        m.crime,
        COUNT(f.crime_key) AS number_of_crimes,
        DENSE_RANK() OVER (PARTITION BY v.victim_sex ORDER BY COUNT(f.crime_key) DESC) AS ranking
    FROM gold.fact_crime_event f
    LEFT JOIN gold.dim_method m ON f.method_key = m.method_key
    LEFT JOIN gold.dim_victim_profile v ON f.victim_profile_key = v.victim_profile_key
    GROUP BY v.victim_sex, m.crime
) final
WHERE ranking <= 10 AND UPPER(victim_sex) IN ('MALE', 'FEMALE');
GO

-- ============================================
--		Crimes by Victim Descent and Area
-- ============================================
IF OBJECT_ID('views.vw_crimes_by_descent_area', 'V') IS NOT NULL
    DROP VIEW views.vw_crimes_by_descent_area;
GO

CREATE VIEW views.vw_crimes_by_descent_area AS
SELECT
    YEAR(f.date_occurred) AS year,
    f.area,
    v.victim_descent,
    COUNT(f.crime_key) AS number_of_crimes
FROM gold.fact_crime_event f
LEFT JOIN gold.dim_victim_profile v ON f.victim_profile_key = v.victim_profile_key
GROUP BY f.area, v.victim_descent, YEAR(f.date_occurred);
GO

-- ============================================
--				Crimes by Location
-- ============================================
IF OBJECT_ID('views.vw_crimes_by_location_coordinates', 'V') IS NOT NULL
    DROP VIEW views.vw_crimes_by_location_coordinates;
GO

CREATE VIEW views.vw_crimes_by_location_coordinates AS
SELECT
    year,
    area,
    AVG(crime_address_latitude) AS average_latitude,
    AVG(crime_address_longitude) AS average_longitude,
    COUNT(dr_no) AS number_of_crimes
FROM (
    SELECT 
        YEAR(f.date_occurred) AS year,
        f.area,
        l.crime_address_latitude,
        l.crime_address_longitude,
        f.dr_no
    FROM gold.fact_crime_event f
    LEFT JOIN gold.dim_location l ON f.location_key = l.location_key
    WHERE l.crime_address_latitude <> 0 AND l.crime_address_longitude <> 0
) sub
GROUP BY year, area;
GO

-- ============================================
--		Top Addresses with Most Crimes
-- ============================================
IF OBJECT_ID('views.vw_top_crime_addresses', 'V') IS NOT NULL
    DROP VIEW views.vw_top_crime_addresses;
GO

CREATE VIEW views.vw_top_crime_addresses AS
SELECT 
    *
FROM (
    SELECT
        YEAR(f.date_occurred) AS year,
        l.crime_address,
        COUNT(f.crime_key) AS number_of_crimes,
        DENSE_RANK() OVER (PARTITION BY YEAR(f.date_occurred) ORDER BY COUNT(f.crime_key) DESC) AS ranking
    FROM gold.fact_crime_event f
    LEFT JOIN gold.dim_location l ON f.location_key = l.location_key
    GROUP BY YEAR(f.date_occurred), l.crime_address
) sub
WHERE ranking <= 10;
GO

-- ============================================
--		Crime Status by Reporting District
-- ============================================
IF OBJECT_ID('views.vw_crime_status_by_district', 'V') IS NOT NULL
    DROP VIEW views.vw_crime_status_by_district;
GO

CREATE VIEW views.vw_crime_status_by_district AS
SELECT 
    YEAR(f.date_occurred) AS year,
    s.reporting_district_number,
    s.status_description,
    COUNT(f.crime_key) AS number_of_crimes
FROM gold.fact_crime_event f
LEFT JOIN gold.dim_status s ON f.status_key = s.status_key
GROUP BY YEAR(f.date_occurred), s.reporting_district_number, s.status_description;
GO
