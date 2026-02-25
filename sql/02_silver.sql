USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;
CREATE OR REPLACE TABLE SILVER.STORE_SALES_CLEAN AS
SELECT
    ss.ss_sold_date_sk,
    ss.ss_store_sk,
    ss.ss_customer_sk,     
    ss.ss_quantity,
    ss.ss_sales_price
FROM BRONZE.STORE_SALES_RAW ss
where ss_sales_price is not null and ss_quantity >0;

-- join SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM dd
-- on ss.ss_sold_date_sk = dd.d_date_sk
-- WHERE dd.d_year between 2000 and 2002;

--DATA VALIDATION
select min(ss_sales_price) as min_sales_price, max(ss_sales_price) as max_sales_price from SILVER.STORE_SALES_CLEAN;

SELECT COUNT(*) AS TOTAL_RECORDS FROM SILVER.STORE_SALES_CLEAN;