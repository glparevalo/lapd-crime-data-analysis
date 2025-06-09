/*
=============================================================================
			Load Silver: Load the Silver Table
=============================================================================

Loading the Silver Table:
	This script is used to load the silver table which consists of the  
	cleaned data of the LAPD Crime Dataset. This table is used to transform
	and normalize the dataset to prepare it for the gold layer. This script 
	truncates the table before inserting data.

3NF Tables:
	This script also creates the split tables.

=============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE 
        @start_time DATETIME = GETDATE(),
	    @end_time DATETIME = GETDATE(),
	    @batch_start_time DATETIME = GETDATE(),
        @section_start_time DATETIME,
        @section_end_time DATETIME;

    PRINT '==============================================';
    PRINT '       STARTING SILVER LAYER LOADING          ';
    PRINT '==============================================';
    
    BEGIN TRY

        -- ================================================
        --              Load Flat Silver Table
        -- ================================================
		PRINT(' ');
        PRINT '========== Flat Silver Table ==========';
        -- Set start time to measure the processing time
        SET @start_time = GETDATE();

        PRINT('> Truncating silver.lapd_crime_data...');
        TRUNCATE TABLE silver.lapd_crime_data;
        
        PRINT('> Inserting data into silver.lapd_crime_data...');
        INSERT INTO silver.lapd_crime_data (
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
            crime_lat,
            crime_lon
        )

        SELECT 
            dr_no,
            date_reported,
            date_occurred,
            time_occurred,
            area AS area_id,
            case
				when UPPER(TRIM(area_name)) = 'N HOLLYWOOD' THEN 'NORTH HOLLYWOOD'
				ELSE UPPER(TRIM(area_name))
			END AS area_name,
            report_district_no,
            part,
            crime_cd,
            UPPER(TRIM(crime_cd_desc)) AS crime_cd_desc,
			TRIM(mo_codes),
            CASE 
                WHEN vict_age < 0 THEN ABS(vict_age)
                ELSE vict_age 
            END as vict_age,
            CASE
                WHEN UPPER(TRIM(vict_sex)) = 'H' THEN 'OTHER'
                WHEN UPPER(TRIM(vict_sex)) = '-' or UPPER(TRIM(vict_sex)) = 'X' or UPPER(TRIM(vict_sex)) IS NULL THEN 'N/A'
                WHEN UPPER(TRIM(vict_sex)) = 'M' THEN 'MALE'
                WHEN UPPER(TRIM(vict_sex)) = 'F' THEN 'FEMALE'
                ELSE UPPER(TRIM(vict_sex))
            END AS vict_sex,
            CASE	
                WHEN UPPER(TRIM(vict_descent)) is null or UPPER(TRIM(vict_descent)) = '-' THEN 'X'
                ELSE UPPER(TRIM(vict_descent))
            END AS vict_descent,
            ISNULL(premis_cd, 0) as premis_cd,
            case
				when premis_cd is null and premis_desc is null then 'N/A'
				when premis_cd is not null and premis_desc is null then 'NO DESCRIPTION GIVEN'
				ELSE premis_desc
			END AS premis_desc,
            ISNULL(weapon_used_cd, 0),
            ISNULL(UPPER(TRIM(weapon_desc)), 'UNKNOWN WEAPON/OTHER WEAPON') AS weapon_desc,
            ISNULL(UPPER(TRIM(status_cd)), 'N/A') AS status_cd,
            CASE
                WHEN UPPER(TRIM(status_cd)) = 'CC' and status_desc = 'UNK' THEN 'Case Closed'
                WHEN UPPER(TRIM(status_cd)) is null and status_desc = 'UNK' OR UPPER(TRIM(status_cd)) = 'N/A' THEN 'Unknown'
                WHEN UPPER(TRIM(status_cd)) = 'JA' THEN 'Juvenile Arrest'
                WHEN UPPER(TRIM(status_cd)) = 'JO' THEN 'Juvenile Other'
                WHEN UPPER(TRIM(status_cd)) = 'AO' THEN 'Adult Other'
                WHEN UPPER(TRIM(status_cd)) = 'AA' THEN 'Adult Arrest'
                WHEN UPPER(TRIM(status_cd)) = 'IC' THEN 'Investigation Continued'
            END AS status_desc,
            TRIM(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(
                    REPLACE(REPLACE(REPLACE(TRIM(crime_location), '  ',' ')
                    , '  ', ' '), '  ', ' '), '  ', ' '),
                '  ', ' '), '  ', ' '), '  ', ' '),
            '  ', ' '), '  ', ' ') + ' ' +
            CASE
				WHEN UPPER(TRIM(cross_street)) IS NULL THEN ''
				ELSE REPLACE(REPLACE(REPLACE(
						REPLACE(REPLACE(REPLACE(
							REPLACE(REPLACE(REPLACE(TRIM(cross_street), '  ',' ')
							, '  ', ' '), '  ', ' '), '  ', ' '),
						'  ', ' '), '  ', ' '), '  ', ' '),
					'  ', ' '), '  ', ' ')
				END) AS crime_location,
			ISNULL(CAST(cast(crime_lat as decimal(8,5)) AS nvarchar(150)), '0') AS crime_lat,
            ISNULL(CAST(cast(crime_lon as decimal(8,5)) AS nvarchar(150)), '0') AS crime_lon
        FROM bronze.lapd_crime_data
        WHERE YEAR(date_occurred) <> 2025;

		SET @end_time = GETDATE();
        PRINT('Completed silver.lapd_crime_data in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.');
		PRINT(' ');

        -- ================================================
        --              FIRST NORMAL FORM
        -- ================================================

        TRUNCATE TABLE silver.norm_lapd_crime_data;
        INSERT INTO silver.norm_lapd_crime_data (
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
            crime_lat,
            crime_lon
        )
        SELECT
            l.dr_no,
            l.date_reported,
            l.date_occurred,
            l.time_occurred,
            l.area,
            l.area_name,
            l.report_district_no,
            l.part,
            l.crime_cd,
            l.crime_cd_desc,
            COALESCE(NULLIF(TRIM(s.value), ''), 'No MO Codes') AS mo_codes,
            vict_age,
            l.vict_sex,
            l.vict_descent,
            premis_cd,
            l.premis_desc,
            l.weapon_used_cd,
            l.weapon_desc,
            l.status_cd,
            status_desc,
            l.crime_location,
            l.crime_lat,
            l.crime_lon
        FROM silver.lapd_crime_data l
        OUTER APPLY STRING_SPLIT(l.mo_codes, ' ') s;

        -- ================================================
        --              Load Dimension: Location
        -- ================================================

        -- Sub-dimension: Area
        PRINT '========== Sub-dim: Area Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.sub_dim_area...');
        TRUNCATE TABLE silver.sub_dim_area;

        PRINT('> Inserting data into silver.sub_dim_area...');
        INSERT INTO silver.sub_dim_area(
            area,
            area_name
        )
        select distinct
            area,
            area_name
        from silver.norm_lapd_crime_data;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.sub_dim_area in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- Sub-dimension: Premise
        PRINT '========== Sub-dimension: Premise Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.sub_dim_premis...');
        TRUNCATE TABLE silver.sub_dim_premis;

        PRINT('> Inserting data into silver.sub_dim_premis...');
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
        order by premis_cd;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.sub_dim_premis in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- Sub-dimension: Address 
        PRINT '========== Sub-dimension: Address Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.sub_dim_address...');
        TRUNCATE TABLE silver.sub_dim_address;

        PRINT('> Inserting data into silver.sub_dim_address...');
        insert into silver.sub_dim_address(
            sk_address_key,
            crime_address,
            crime_lat,
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
        order by sk_address_key;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.sub_dim_address in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- Dimension: Location
        PRINT '========== Dim: Location ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.dim_location...');
        TRUNCATE TABLE silver.dim_location;

        PRINT('> Inserting data into silver.dim_location...');
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
            AND M.crime_lon = D.crime_lon) a;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.dim_location in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --               Load Dimension: Method
        -- ================================================
        
        -- Sub-dimension: Weapon
        PRINT '========== Sub-dimension: Weapon Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.sub_dim_weapon...');
        TRUNCATE TABLE silver.sub_dim_weapon;

        PRINT('> Inserting data into silver.sub_dim_weapon...');
        INSERT INTO silver.sub_dim_weapon(
            weapon_used_cd,
            weapon_desc
        )
        select distinct
            weapon_used_cd,
            weapon_desc
        from silver.norm_lapd_crime_data
        order by weapon_used_cd;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.weapon_table in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- Sub-dimension: Crime
        PRINT '========== Sub-dimension: Crime Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.sub_dim_crime...');
        TRUNCATE TABLE silver.sub_dim_crime;

        PRINT('> Inserting data into silver.sub_dim_crime...');
        insert into silver.sub_dim_crime(
            crime_cd,
            crime_cd_desc
        )
        select distinct
            crime_cd,
            crime_cd_desc
        from silver.norm_lapd_crime_data
        order by crime_cd;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.sub_dim_crime in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- Sub-dimension: Part
        PRINT '========== Sub-dimension: Part Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.sub_dim_part...');
        TRUNCATE TABLE silver.sub_dim_part;

        PRINT('> Inserting data into silver.sub_dim_part...');
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

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.sub_dim_part in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- Dimension: Method
        PRINT '========== Dimension: Method Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.dim_method...');
        TRUNCATE TABLE silver.dim_method;

        PRINT('> Inserting data into silver.dim_method...');
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
            weapon_used_cd
        from silver.norm_lapd_crime_data) a;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.dim_method in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

	    -- ================================================
        --          Load Dimension: Victim Profile
        -- ================================================

        -- Sub-dimension: Descent
        PRINT '========== Sub-dimension: Descent Table ==========';        
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.sub_dim_descent...');
        TRUNCATE TABLE silver.sub_dim_descent;

        PRINT('> Inserting data into silver.sub_dim_descent...');
        INSERT INTO silver.sub_dim_descent(
            vict_descent,
            vict_descent_desc
        )
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
        from silver.norm_lapd_crime_data;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.sub_dim_descent in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- Dimension: Victim Profile
        PRINT '========== Dimension: Victim Profile Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.dim_victim_profile...');
        TRUNCATE TABLE silver.dim_victim_profile;

        PRINT('> Inserting data into silver.dim_victim_profile...');
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
        FROM (
        select distinct
            vict_age,
            vict_sex,
            vict_descent
        from silver.norm_lapd_crime_data) a;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.dim_victim_profile in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --             Load Dimension: Status
        -- ================================================

        -- Dimension: Status
        PRINT '========== Dimension: Status Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.dim_status...');

        TRUNCATE TABLE silver.dim_status;

        PRINT('> Inserting data into silver.dim_status...');

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
        from silver.norm_lapd_crime_data;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.dim_status in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --             Load Dimension: Time
        -- ================================================

        -- Dimension: Time
        PRINT '========== Dimension: Time Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.dim_time...');
        TRUNCATE TABLE silver.dim_time;

        PRINT('> Inserting data into silver.dim_time...');
        insert into silver.dim_time(
            sk_time_key,
            date_occurred,
            time_occurred	
        )

        select
            ROW_NUMBER() over (order by date_occurred, time_occurred) as sk_time_key,
            date_occurred,
            time_occurred
        FROM (
        select distinct
            date_occurred,
            time_occurred
        from silver.norm_lapd_crime_data) a
        order by sk_time_key;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.dim_status in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --             Load Dimension: MO Codes
        -- ================================================

        -- Dimension: MO Codes
        PRINT '========== Dimension: MO Code Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.dim_mo_code...');
        TRUNCATE TABLE silver.dim_mo_code;

        PRINT('> Inserting data into silver.dim_mo_code...');
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
        from silver.norm_lapd_crime_data;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.dim_mo_code in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --            Load Fact: Specifics Table
        -- ================================================
       
        -- Fact: Specifics
        PRINT '========== Load Fact: Specifics Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.fact_specifics...');
        TRUNCATE TABLE silver.fact_specifics;

        PRINT('> Inserting data into silver.fact_specifics...');
        with cte as (
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

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.crime_setting in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --              Final Batch Summary
        -- ================================================
        DECLARE @batch_end_time DATETIME = GETDATE();
        PRINT '==============================================';
        PRINT 'SILVER LAYER NORMALIZATION COMPLETED SUCCESSFULLY.';
        PRINT 'Total time: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
        PRINT '==============================================';

    END TRY
    BEGIN CATCH
        PRINT '==============================================';
        PRINT 'ERROR OCCURRED DURING SILVER LAYER NORMALIZATION';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==============================================';
    END CATCH
END;
