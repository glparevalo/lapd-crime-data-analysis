CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    DECLARE
        @batch_start_time DATETIME = GETDATE(),
        @start_time DATETIME,
        @end_time DATETIME;

    PRINT '==============================================';
    PRINT '         STARTING GOLD LAYER LOADING           ';
    PRINT '==============================================';

    BEGIN TRY

        -- ================================================
        --            Load Location Table
        -- ================================================
        PRINT(' ');
		PRINT '========== Location Table (Dim) ==========';
        SET @start_time = GETDATE();

        PRINT '> Truncating gold.dim_location...';
        TRUNCATE TABLE gold.dim_location;

        PRINT '> Inserting data into gold.dim_location...';
        INSERT INTO gold.dim_location(
            location_key,
            premise,
            crime_address,
            crime_address_latitude,
            crime_address_longitude
        )
        SELECT
            m.sk_location_key,
            p.premis_desc,
            m.crime_location,
            CAST(m.crime_lat AS DECIMAL(8, 5)),
            CAST(m.crime_lon AS DECIMAL(8, 5))
        FROM silver.location_table AS m
        LEFT JOIN silver.premis_table AS p ON m.premis_cd = p.premis_cd;

        SET @end_time = GETDATE();
        PRINT 'Completed gold.dim_location in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT ' ';

        -- ================================================
        --            Load Victim Profile Table
        -- ================================================
        PRINT '========== Victim Profile Table (Dim) ==========';
        SET @start_time = GETDATE();

        PRINT '> Truncating gold.dim_victim_profile...';
        TRUNCATE TABLE gold.dim_victim_profile;

        PRINT '> Inserting data into gold.dim_victim_profile...';
        INSERT INTO gold.dim_victim_profile(
			victim_profile_key,
            victim_age,
            victim_sex,
            victim_descent
        )
        SELECT
            m.sk_victim_profile_key,
            m.vict_age,
            m.vict_sex,
            v.vict_descent_desc
        FROM silver.crime_victim_profile AS m
        LEFT JOIN silver.victim_table AS v ON m.vict_descent = v.vict_descent;

        SET @end_time = GETDATE();
        PRINT 'Completed gold.dim_victim_profile in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT ' ';

        -- ================================================
        --					Load Status Table
        -- ================================================
        PRINT '========== Status Table (Dim) ==========';
        SET @start_time = GETDATE();

        PRINT '> Truncating gold.dim_status...';
        TRUNCATE TABLE gold.dim_status;

        PRINT '> Inserting data into gold.dim_status...';
        INSERT INTO gold.dim_status(
            status_key,
            reporting_district_number,
            status_description
        )
        SELECT
            m.sk_crime_status_key,
            m.report_district_no,
            s.status_desc
        FROM silver.crime_status AS m
        LEFT JOIN silver.status_table AS s ON m.status_cd = s.status_cd;

        SET @end_time = GETDATE();
        PRINT 'Completed gold.dim_status in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT ' ';

        -- ================================================
        --				  Load Method Table
        -- ================================================
        PRINT '========== Method Table (Dim) ==========';
        SET @start_time = GETDATE();

        PRINT '> Truncating gold.dim_method...';
        TRUNCATE TABLE gold.dim_method;

        PRINT '> Inserting data into gold.dim_method...';
        INSERT INTO gold.dim_method(
            method_key,
            part,
            category,
            crime,
            weapon_used
        )
        SELECT
            m.sk_crime_method_key,
            p.part_name,
            p.category,
            c.crime_desc,
            w.weapon_used_desc
        FROM silver.crime_method AS m
        LEFT JOIN silver.crime_table AS c ON m.crime_cd = c.crime_cd
        LEFT JOIN silver.weapon_table AS w ON m.weapon_used_cd = w.weapon_used_cd
        LEFT JOIN silver.part_table AS p ON m.part = p.part;

        SET @end_time = GETDATE();
        PRINT 'Completed gold.dim_method in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT ' ';

        -- ================================================
        --            Load Crime Event Table
        -- ================================================
        PRINT '========== Crime Event Table (Fact) ==========';
        SET @start_time = GETDATE();

        PRINT '> Truncating gold.fact_crime_event...';
        TRUNCATE TABLE gold.fact_crime_event;

        PRINT '> Inserting data into gold.fact_crime_event...';
        INSERT INTO gold.fact_crime_event(
            crime_key,
            dr_no,
            date_reported,
            date_occurred,
            time_occurred,
            area,
            location_key,
            method_key,
            victim_profile_key,
            status_key
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY m.dr_no, m.date_reported),
            m.dr_no,
            m.date_reported,
            m.date_occurred,
            m.time_occurred,
            a.area_name,
            m.sk_location_key,
            m.sk_crime_method_key,
            m.sk_victim_profile_key,
            m.sk_crime_status_key
        FROM silver.crime_setting AS m
        LEFT JOIN silver.area_table AS a ON m.area_id = a.area_id;

        SET @end_time = GETDATE();
        PRINT 'Completed gold.fact_crime_event in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT ' ';

        -- ================================================
        --              Final Batch Summary
        -- ================================================
        DECLARE @batch_end_time DATETIME = GETDATE();
        PRINT '==============================================';
        PRINT 'GOLD LAYER LOADING COMPLETED SUCCESSFULLY';
        PRINT 'Total Time: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==============================================';

    END TRY
    BEGIN CATCH
        PRINT '==============================================';
        PRINT 'ERROR OCCURRED DURING GOLD LAYER LOADING';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==============================================';
    END CATCH
END;