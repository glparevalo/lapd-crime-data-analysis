/* 
=============================================================================
						Loading the Bronze Table
=============================================================================

Bronze Table:
	This script is used to create or alter a stored procedure that will 
	insert data into the Bronze table which consists of the raw data 
	of the LAPD Crime Dataset. 
	
	* This procedure will truncate the table before inserting values
	* It uses the bulk insert command to load data from csv files
	
How to Use:
	EXEC bronze.load_bronze;

=============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time datetime, @end_time datetime;
    BEGIN TRY

        PRINT'==========================================';
        PRINT('		     Loading Bronze Table           ');
        PRINT'==========================================';

        -- Set start time to measure the processing time
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.lapd_crime_data;
        PRINT('Truncating bronze.lapd_crime_data...');

        PRINT('Inserting data into bronze.lapd_crime_data...');
        BULK INSERT bronze.lapd_crime_data
        FROM 'C:\output_pipe.txt'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '|',
			ROWTERMINATOR = '\n',
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

exec bronze.load_bronze