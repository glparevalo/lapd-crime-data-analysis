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
            area,
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
            cross_street,
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
            REPLACE(REPLACE(UPPER(TRIM(crime_cd_desc)), ';', ': '), '  ',' ') AS new,
            CASE 
                WHEN vict_age < 0 THEN ABS(vict_age)
                WHEN vict_age = 0 THEN null
                ELSE vict_age 
            END AS vict_age,
            CASE
                WHEN UPPER(TRIM(vict_sex)) = 'H' THEN 'Other'
                WHEN UPPER(TRIM(vict_sex)) = '-' or UPPER(trim(vict_sex)) = 'X' or upper(trim(vict_sex)) IS NULL THEN 'N/A'
                WHEN UPPER(TRIM(vict_sex)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(vict_sex)) = 'F' THEN 'Female'
                ELSE UPPER(TRIM(vict_sex))
            END AS vict_sex,
            CASE	
                WHEN UPPER(trim(vict_descent)) is null or UPPER(trim(vict_descent)) = '-' THEN 'X'
                ELSE UPPER(trim(vict_descent))
            END AS vict_descent,
            premis_cd,
            UPPER(trim(premis_desc)) AS premis_desc,
            weapon_used_cd,
            UPPER(trim(weapon_desc)) AS weapon_desc,
            UPPER(trim(status_cd)) AS status_cd,
            CASE
                WHEN UPPER(TRIM(status_cd)) = 'CC' and status_desc = 'UNK' THEN 'Case Closed'
                WHEN UPPER(TRIM(status_cd)) is null and status_desc = 'UNK' THEN 'Unknown'
                WHEN UPPER(TRIM(status_cd)) = 'JA' THEN 'Juvenile Arrest'
                WHEN UPPER(TRIM(status_cd)) = 'JO' THEN 'Juvenile Other'
                WHEN UPPER(TRIM(status_cd)) = 'AO' THEN 'Adult Other'
                WHEN UPPER(TRIM(status_cd)) = 'AA' THEN 'Adult Arrest'
                WHEN UPPER(TRIM(status_cd)) = 'IC' THEN 'Investigation Continued'
            END AS status_desc,
            REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(
                    REPLACE(REPLACE(REPLACE(TRIM(crime_location), '  ',' ')
                    , '  ', ' '), '  ', ' '), '  ', ' '),
                '  ', ' '), '  ', ' '), '  ', ' '),
            '  ', ' '), '  ', ' ') AS crime_location,
            CASE
				WHEN UPPER(TRIM(cross_street)) IS NULL THEN ''
				ELSE REPLACE(REPLACE(REPLACE(
						REPLACE(REPLACE(REPLACE(
							REPLACE(REPLACE(REPLACE(TRIM(cross_street), '  ',' ')
							, '  ', ' '), '  ', ' '), '  ', ' '),
						'  ', ' '), '  ', ' '), '  ', ' '),
					'  ', ' '), '  ', ' ')
				END AS cross_street,
            crime_lat,
            crime_lon
        FROM bronze.lapd_crime_database

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

GO

EXEC silver.load_silver_flat_db