/* 
=============================================================================
						Loading the Bronze Table
=============================================================================

Normalization Table:
	This script is used to create or alter a stored procedure that will 
	insert data into the Bronze table which consists of the raw data 
	of the LAPD Crime Dataset. 
	
	* This procedure will truncate the table before inserting values
	* It uses the bulk insert command to load data from csv files
	
How to Use:
	EXEC bronze.load_normalization;

=============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_normalization AS
BEGIN
    DECLARE @start_time datetime, @end_time datetime;
    BEGIN TRY

        PRINT'==========================================';
        PRINT('		     Loading Bronze Table           ');
        PRINT'==========================================';

        -- Set start time to measure the processing time
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.lapd_crime_database;
        PRINT('Truncating bronze.lapd_crime_database...');

        PRINT('Inserting data into bronze.lapd_crime_database...');
        BULK INSERT bronze.lapd_crime_database
        FROM 'C:\lapd_crime_dataset_cleaned.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
			CODEPAGE = '65001',
            TABLOCK
        );

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

exec bronze.load_normalization