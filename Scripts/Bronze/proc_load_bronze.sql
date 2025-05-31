## 📦 Bronze Layer Loader

This procedure (`load_bronze`) automates the batch loading of structured source data (CRM and ERP systems) into the `bronze` schema of a data warehouse. It performs the following steps:

- Truncates existing Bronze tables
- Loads CSV files using PostgreSQL's `COPY` command
- Logs key stages of the process
- Captures and reports any errors

**Input**: CSV files located in the server filesystem  
**Output**: Refreshed Bronze tables ready for further ETL processing


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMPTZ;
    batch_end_time TIMESTAMPTZ;
BEGIN
    batch_start_time := NOW();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    -- CRM TABLES
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    RAISE NOTICE '>> Truncating Table: Bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: Bronze.crm_cust_info';
    COPY bronze.crm_cust_info FROM 'C:/Users/Aniket/Downloads/datasets/source_crm/cust_info.csv' DELIMITER ',' CSV HEADER;

    RAISE NOTICE '>> Truncating Table: Bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: Bronze.crm_prd_info';
    COPY bronze.crm_prd_info FROM 'C:/Users/Aniket/Downloads/datasets/source_crm/prd_info.csv' DELIMITER ',' CSV HEADER;

    RAISE NOTICE '>> Truncating Table: Bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: Bronze.crm_sales_details';
    COPY bronze.crm_sales_details FROM 'C:/Users/Aniket/Downloads/datasets/source_crm/sales_details.csv' DELIMITER ',' CSV HEADER;

    -- ERP TABLES
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    RAISE NOTICE '>> Truncating Table: Bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: Bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101 FROM 'C:/Users/Aniket/Downloads/datasets/source_erp/loc_a101.csv' DELIMITER ',' CSV HEADER;

    RAISE NOTICE '>> Truncating Table: Bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: Bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12 FROM 'C:/Users/Aniket/Downloads/datasets/source_erp/cust_az12.csv' DELIMITER ',' CSV HEADER;

    RAISE NOTICE '>> Truncating Table: Bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: Bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2 FROM 'C:/Users/Aniket/Downloads/datasets/source_erp/px_cat_g1v2.csv' DELIMITER ',' CSV HEADER;

    batch_end_time := NOW();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(SECOND FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING Bronze LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
END;
$$;
