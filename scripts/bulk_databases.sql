/*
==========================================================================================
Bulk information into tables
==========================================================================================
Script Purpose:
  The script bulk the data into bronze schema tables..

!Warning:
  Running this script will drop all data in the tables and fill with the new info.
  All the old data in the tables will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.
*/



CREATE or ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    PRINT '==========================================================';
    PRINT 'Loading Bronze Layer';
    PRINT '==========================================================';
    PRINT '----------------------------------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '----------------------------------------------------------';

    PRINT '>> Truncating Table: bronze.crm_cust_info';

    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT '>> Inserting data into: bronze.crm_cust_info';

    BULK INSERT bronze.crm_cust_info
    from '/var/opt/mssql/data/source_crm/cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );   

    PRINT '>> Truncating Table: bronze.crm_sales_details';

    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT '>> Inserting data into: bronze.crm_sales_details';

    BULK INSERT bronze.crm_sales_details
    from '/var/opt/mssql/data/source_crm/sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );    

    PRINT '>> Truncating Table: bronze.crm_prd_info';

    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT '>> Inserting data into: bronze.crm_prd_info';

    BULK INSERT bronze.crm_prd_info
    from '/var/opt/mssql/data/source_crm/prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    ); 

    PRINT '----------------------------------------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '----------------------------------------------------------';

    PRINT '>> Truncating Table: bronze.erp_cust_az12';

    TRUNCATE TABLE bronze.erp_cust_az12;

    PRINT '>> Inserting data into: bronze.erp_cust_az12';

    BULK INSERT bronze.erp_cust_az12
    from '/var/opt/mssql/data/source_erp/cust_az12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );

    PRINT '>> Truncating Table: bronze.erp_loc_a101';

    TRUNCATE TABLE bronze.erp_loc_a101;

    PRINT '>> Inserting data into: bronze.erp_loc_a101';

    BULK INSERT bronze.erp_loc_a101
    from '/var/opt/mssql/data/source_erp/loc_a101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );

    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2';

    BULK INSERT bronze.erp_px_cat_g1v2
    from '/var/opt/mssql/data/source_erp/px_cat_g1v2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
END
