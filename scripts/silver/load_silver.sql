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
        TRUNCATE TABLE silver.lapd_crime_database;
        
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
        FROM bronze.lapd_crime_data;

		SET @end_time = GETDATE();
        PRINT('Completed silver.lapd_crime_data in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.');
		PRINT(' ');

        -- ================================================
        --              FIRST NORMAL FORM
        -- ================================================

        TRUNCATE TABLE silver.lapd_crime_database;
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
            -- If mo_codes exist, use the trimmed value; else 'No MO Codes'
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
        --              Load Location Table
        -- ================================================
        
        PRINT '========== Silver Location Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.location_table...');
        TRUNCATE TABLE silver.location_table;

        PRINT('> Inserting data into silver.location_table...');
        INSERT INTO silver.location_table (
            sk_location_key, 
            premis_cd, 
            crime_location, 
            crime_lat, 
            crime_lon
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY crime_location, premis_cd),
            premis_cd, 
            crime_location, 
            crime_lat, 
            crime_lon
        FROM (
            SELECT DISTINCT 
                premis_cd, 
                crime_location, 
                crime_lat, 
                crime_lon
            FROM silver.lapd_crime_database) t;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.location_table in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --                Load Area Table
        -- ================================================
        
        PRINT '========== Area Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.area_table...');
        TRUNCATE TABLE silver.area_table;

        PRINT('> Inserting data into silver.area_table...');
        INSERT INTO silver.area_table (
            area_id, 
            area_name
        )
        SELECT DISTINCT 
            area_id, 
            area_name
        FROM silver.lapd_crime_database
        ORDER BY area_id;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.area_table in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --              Load Premise Table
        -- ================================================
        
        PRINT '========== Premise Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.premis_table...');
        TRUNCATE TABLE silver.premis_table;

        PRINT('> Inserting data into silver.premis_table...');
        INSERT INTO silver.premis_table (
            premis_cd, 
            premis_desc
        )
        SELECT DISTINCT 
            premis_cd, 
            premis_desc
        FROM silver.lapd_crime_database
        ORDER BY premis_cd;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.premis_table in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --               Load Weapon Table
        -- ================================================
        
        PRINT '========== Weapon Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.weapon_table...');
        TRUNCATE TABLE silver.weapon_table;

        PRINT('> Inserting data into silver.weapon_table...');
        INSERT INTO silver.weapon_table (
            weapon_used_cd, 
            weapon_used_desc
        )
        SELECT DISTINCT 
            weapon_used_cd, 
            weapon_desc
        FROM silver.lapd_crime_database
        ORDER BY weapon_used_cd;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.weapon_table in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --                Load Crime Table
        -- ================================================

        PRINT '========== Crime Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.crime_table...');
        TRUNCATE TABLE silver.crime_table;

        PRINT('> Inserting data into silver.crime_table...');
        INSERT INTO silver.crime_table (
            crime_cd, 
            crime_desc
        )
        SELECT DISTINCT 
            crime_cd, 
            crime_cd_desc
        FROM silver.lapd_crime_database
        ORDER BY crime_cd;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.crime_table in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

	-- ================================================
        --               Load Victim Table
        -- ================================================

        PRINT '========== Victim Table ==========';        
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.victim_table...');
        TRUNCATE TABLE silver.victim_table;

        PRINT('> Inserting data into silver.victim_table...');
        INSERT INTO silver.victim_table (
            vict_descent, 
            vict_descent_desc
        )
        SELECT DISTINCT 
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
        FROM silver.lapd_crime_database
        ORDER BY vict_descent;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.crime_table in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --            Load Crime Method Table
        -- ================================================
        
        PRINT '========== Crime Method Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.crime_method...');
        TRUNCATE TABLE silver.crime_method;

        PRINT('> Inserting data into silver.crime_method...');
        INSERT INTO silver.crime_method (
            sk_crime_method_key, 
            part, 
            crime_cd, 
            weapon_used_cd
        )
        SELECT ROW_NUMBER() OVER (ORDER BY part, crime_cd, weapon_used_cd),
               part, 
               crime_cd, 
               weapon_used_cd
        FROM (
            SELECT DISTINCT 
                part, 
                crime_cd, 
                weapon_used_cd
            FROM silver.lapd_crime_database
        ) t;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.crime_method in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --                  Load Part Table
        -- ================================================

        PRINT '========== Part Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.part_table...');
        TRUNCATE TABLE silver.part_table;

        PRINT('> Inserting data into silver.part_table...');
        INSERT INTO silver.part_table (
            part,
	        part_name,
	        category
        )
        SELECT DISTINCT 
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
            END AS category
        FROM silver.lapd_crime_database

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.part_table in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --          Load Crime Victim Profile
        -- ================================================
        
        PRINT '========== Crime Victim Profile Table ==========';

        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.crime_victim_profile...');
        TRUNCATE TABLE silver.crime_victim_profile;

        PRINT('> Inserting data into silver.crime_victim_profile...');
        INSERT INTO silver.crime_victim_profile (
            sk_victim_profile_key, 
            vict_age, 
            vict_sex, 
            vict_descent
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY vict_age, vict_sex, vict_descent),
            vict_age, 
            vict_sex, 
            vict_descent
        FROM (
            SELECT DISTINCT 
                vict_age, 
                vict_sex, 
                vict_descent
            FROM silver.lapd_crime_database
        ) t;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.crime_victim_profile in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --               Load Status Table
        -- ================================================

        PRINT '========== Premise Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.status_table...');

        TRUNCATE TABLE silver.status_table;

        PRINT('> Inserting data into silver.status_table...');

        INSERT INTO silver.status_table (
            status_cd, 
            status_desc
        )
        SELECT DISTINCT 
            status_cd, 
            status_desc
        FROM silver.lapd_crime_database;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.status_table in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --           Load Crime Status Table
        -- ================================================

        PRINT '========== Crime Status Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.crime_status...');
        TRUNCATE TABLE silver.crime_status;

        PRINT('> Inserting data into silver.crime_status...');
        INSERT INTO silver.crime_status (
            sk_crime_status_key, 
            report_district_no, 
            status_cd
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY report_district_no, status_cd),
            report_district_no, 
            status_cd
        FROM (SELECT DISTINCT report_district_no, status_cd
              FROM silver.lapd_crime_database) a;

        SET @section_end_time = GETDATE();
        PRINT 'Completed silver.crime_status in ' + CAST(DATEDIFF(SECOND, @section_start_time, @section_end_time) AS NVARCHAR) + ' seconds.';
		PRINT(' ');

        -- ================================================
        --            Load Crime Setting Table
        -- ================================================
       
        PRINT '========== Crime Setting Table ==========';
        
        SET @section_start_time = GETDATE();

        PRINT('> Truncating silver.crime_setting...');
        TRUNCATE TABLE silver.crime_setting;

        PRINT('> Inserting data into silver.crime_setting...');
        INSERT INTO silver.crime_setting (
            dr_no, 
            date_reported, 
            date_occurred, 
            time_occurred, 
            area_id,
            sk_location_key, 
            sk_crime_method_key, 
            sk_victim_profile_key, 
            sk_crime_status_key
        )
        SELECT 
            m.dr_no, 
            m.date_reported, 
            m.date_occurred, 
            m.time_occurred, 
            m.area_id,
            l.sk_location_key, 
            c.sk_crime_method_key, 
            v.sk_victim_profile_key, 
            s.sk_crime_status_key
        FROM silver.lapd_crime_database m
        LEFT JOIN silver.location_table l
            ON m.premis_cd = l.premis_cd
            AND m.crime_location = l.crime_location
            AND m.crime_lat = l.crime_lat
            AND m.crime_lon = l.crime_lon
        LEFT JOIN silver.crime_method c
            ON m.part = c.part
            AND m.crime_cd = c.crime_cd
            AND m.weapon_used_cd = c.weapon_used_cd
        LEFT JOIN silver.crime_victim_profile v
            ON m.vict_age = v.vict_age
            AND m.vict_sex = v.vict_sex
            AND m.vict_descent = v.vict_descent
        LEFT JOIN silver.crime_status s
            ON m.status_cd = s.status_cd
            AND m.report_district_no = s.report_district_no;

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
