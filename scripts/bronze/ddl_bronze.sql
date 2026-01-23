/* 
=============================================================================
DDL Script: Create bronze tables 
Script Purpose: 
    This script creates tables in the "bronze" schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of "bronze" tables
=============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_error_message TEXT;
    v_error_detail TEXT;
    v_error_hint TEXT;
    v_error_context TEXT;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_table_start TIMESTAMP;
    v_table_name TEXT;
    
    -- Function to calculate and log duration
    v_log_duration TEXT := $inner$
        CREATE OR REPLACE FUNCTION log_duration(p_start_time TIMESTAMP, p_operation TEXT)
        RETURNS VOID AS $func$
        DECLARE
            v_duration INTERVAL;
        BEGIN
            v_duration := clock_timestamp() - p_start_time;
            RAISE NOTICE 'Completed % in %',
                p_operation,
                format('%%s hours %%s minutes %%s seconds',
                       EXTRACT(HOUR FROM v_duration)::INT,
                       EXTRACT(MINUTE FROM v_duration)::INT,
                       ROUND(EXTRACT(SECOND FROM v_duration)::NUMERIC, 2));
        END;
        $func$ LANGUAGE plpgsql;
    $inner$;
BEGIN
    -- Create the log_duration function
    EXECUTE v_log_duration;
    
    -- Record start time
    v_start_time := clock_timestamp();
    RAISE NOTICE 'ETL process started at: %', v_start_time;

    BEGIN
        -- Load crm_cust_info
        v_table_name := 'crm_cust_info';
        v_table_start := clock_timestamp();
        RAISE NOTICE 'Starting to load %...', v_table_name;
        
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
            FROM '/Users/josh/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Loaded % rows into %', (SELECT COUNT(*) FROM bronze.crm_cust_info), v_table_name;
        PERFORM log_duration(v_table_start, 'loading ' || v_table_name);

        -- Load crm_prd_info
        v_table_name := 'crm_prd_info';
        v_table_start := clock_timestamp();
        RAISE NOTICE 'Starting to load %...', v_table_name;
        
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
            FROM '/Users/josh/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Loaded % rows into %', (SELECT COUNT(*) FROM bronze.crm_prd_info), v_table_name;
        PERFORM log_duration(v_table_start, 'loading ' || v_table_name);

        -- Process crm_sales_details
        v_table_name := 'crm_sales_details';
        v_table_start := clock_timestamp();
        RAISE NOTICE 'Starting to process %...', v_table_name;
        
        BEGIN
            ALTER TABLE bronze.crm_sales_details
                ALTER COLUMN sls_ord_num TYPE VARCHAR(20);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Column type change not needed or failed: %', SQLERRM;
        END;

        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details
            FROM '/Users/josh/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Loaded % rows into %', (SELECT COUNT(*) FROM bronze.crm_sales_details), v_table_name;
        PERFORM log_duration(v_table_start, 'processing ' || v_table_name);

        -- Load erp_cust_az12
        v_table_name := 'erp_cust_az12';
        v_table_start := clock_timestamp();
        RAISE NOTICE 'Starting to load %...', v_table_name;
        
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
            FROM '/Users/josh/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Loaded % rows into %', (SELECT COUNT(*) FROM bronze.erp_cust_az12), v_table_name;
        PERFORM log_duration(v_table_start, 'loading ' || v_table_name);

        -- Load erp_loc_a101
        v_table_name := 'erp_loc_a101';
        v_table_start := clock_timestamp();
        RAISE NOTICE 'Starting to load %...', v_table_name;
        
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
            FROM '/Users/josh/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Loaded % rows into %', (SELECT COUNT(*) FROM bronze.erp_loc_a101), v_table_name;
        PERFORM log_duration(v_table_start, 'loading ' || v_table_name);

        -- Process erp_px_cat_g1v2
        v_table_name := 'erp_px_cat_g1v2';
        v_table_start := clock_timestamp();
        RAISE NOTICE 'Starting to process %...', v_table_name;
        
        BEGIN
            ALTER TABLE bronze.erp_px_cat_g1v2
                ADD COLUMN IF NOT EXISTS maintenance VARCHAR(50);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Column addition failed: %', SQLERRM;
        END;

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
            FROM '/Users/josh/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Loaded % rows into %', (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2), v_table_name;
        PERFORM log_duration(v_table_start, 'processing ' || v_table_name);

        -- Calculate and log total duration
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        
        -- Return summary
        RAISE NOTICE 'Load process completed successfully at: %', v_end_time;
        RAISE NOTICE 'Total ETL duration: %', 
            format('%%s hours %%s minutes %%s seconds',
                   EXTRACT(HOUR FROM v_duration)::INT,
                   EXTRACT(MINUTE FROM v_duration)::INT,
                   ROUND(EXTRACT(SECOND FROM v_duration)::NUMERIC, 2));

    EXCEPTION WHEN OTHERS THEN
        -- Record error time
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        
        GET STACKED DIAGNOSTICS
            v_error_message = MESSAGE_TEXT,
            v_error_detail = PG_EXCEPTION_DETAIL,
            v_error_hint = PG_EXCEPTION_HINT,
            v_error_context = PG_EXCEPTION_CONTEXT;

        RAISE EXCEPTION 'Error in load_bronze procedure while processing %: %
Time Elapsed: %
DETAIL: %
HINT: %
CONTEXT: %',
        v_table_name,
        v_error_message,
        format('%%s hours %%s minutes %%s seconds',
               EXTRACT(HOUR FROM v_duration)::INT,
               EXTRACT(MINUTE FROM v_duration)::INT,
               ROUND(EXTRACT(SECOND FROM v_duration)::NUMERIC, 2)),
        v_error_detail,
        v_error_hint,
        v_error_context;
    END;
    
    -- Clean up the temporary function
    DROP FUNCTION IF EXISTS log_duration(TIMESTAMP, TEXT);
END;
$procedure$;
