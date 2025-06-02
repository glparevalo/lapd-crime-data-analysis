/*
=============================================================================
				  Load Silver: Load the Flat Silver Table
=============================================================================

Silver Flat Table:
	This script is used to insert values into the silver table which consists 
    of the cleaned data of the LAPD Crime Dataset. This table is used to 
    normalize the dataset to make it business-ready. This script truncates 
    the table then inserts the data.

=============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver_flat_db AS
BEGIN
    DECLARE @start_time datetime, @end_time datetime;
    BEGIN TRY

        PRINT'==========================================';
        PRINT('		     Loading Silver Table           ');
        PRINT'==========================================';

        -- Set start time to measure the processing time
        SET @start_time = GETDATE();

        

        TRUNCATE TABLE silver.lapd_crime_database;
        PRINT('Truncating silver.lapd_crime_database...');

        PRINT('Inserting data into silver.lapd_crime_database...');
        INSERT INTO silver.lapd_crime_database (
            dr_no,
            date_reported,
            date_occurred,
            time_occurred,
            area_id,
            area_name,
            report_district_no,
            part,
            crime_cd,
            crime_cd_desc,
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
            cast(time_occurred AS TIME(0)) AS time_occurred,
            area AS area_id,
            UPPER(TRIM(area_name)) AS area_name,
            report_district_no,
            part,
            crime_cd,
            REPLACE(REPLACE(UPPER(TRIM(crime_cd_desc)), ';', ': '), '  ',' ') AS crime_cd_desc,
            CASE 
                WHEN vict_age < 0 THEN ABS(vict_age)
                ELSE vict_age 
            END as vict_age,
            CASE
                WHEN UPPER(TRIM(vict_sex)) = 'H' THEN 'Other'
                WHEN UPPER(TRIM(vict_sex)) = '-' or UPPER(TRIM(vict_sex)) = 'X' or UPPER(TRIM(vict_sex)) IS NULL THEN 'N/A'
                WHEN UPPER(TRIM(vict_sex)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(vict_sex)) = 'F' THEN 'Female'
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
            ISNULL(UPPER(TRIM(weapon_desc)), 'N/A') AS weapon_desc,
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
        FROM bronze.lapd_crime_database;

		SET @end_time = GETDATE();
        PRINT('Done processing ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;


create or alter PROCEDURE silver.load_silver_normalized_db as 
    BEGIN
        TRY


    -- Load Location Table    
    TRUNCATE TABLE silver.location_table;

    INSERT INTO silver.location_table(
        sk_location_key,
        premis_cd,
        crime_location,
        crime_lat,
        crime_lon
    )

    select
        ROW_NUMBER() over (order by crime_location, premis_cd) as sk_location_key,
        premis_cd,
        crime_location,
        crime_lat,
        crime_lon 
    from(
    select distinct
        premis_cd,
        crime_location,
        crime_lat,
        crime_lon
    from silver.lapd_crime_database
    ) t

    GO

    -- Load Area Table
    TRUNCATE TABLE silver.area_table;

    INSERT INTO silver.area_table(
        area,
        area_name
    )
    select distinct
        area,
        area_name
    from silver.lapd_crime_database
    order by area;

    GO

    -- Load Premise Table
    TRUNCATE TABLE silver.premis_table;

    INSERT INTO silver.premis_table(
        premis_cd,
        premis_desc
    )
    select distinct
        premis_cd,
        premis_desc
    from silver.lapd_crime_database
	order bY premis_cd


    TRUNCATE TABLE silver.crime_setting;
    insert into silver.crime_setting(
        dr_no,
        date_reported,
        date_occurred,
        time_occurred,
        area,
        sk_location_key
    )

    select 
        m.dr_no,
        m.date_reported,
        date_occurred,
        time_occurred,
        area,
        l.sk_location_key
    from silver.lapd_crime_database m
    left join silver.location_table l
    on m.premis_cd = l.premis_cd
    and m.crime_location = l.crime_location
    and m.crime_lat = l.crime_lat
    and m.crime_lon = l.crime_lon

    /*
    ========================================================

    ========================================================
    */


    end TRY
    begin CATCH
    end catch



end
