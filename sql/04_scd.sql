DESC TABLE SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER;

DESC TABLE SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS;


use warehouse compute_wh;
use database snowflake_learning_db;

create or replace table silver.customer_dim(
    customer_sk number autoincrement,
    customer_id number,
    state string,
    start_date date,
    end_date date,
    is_current string
);

--initial load

insert into silver.customer_dim(customer_id, state, start_date, end_date, is_current)
select
    c_customer_sk,
    ca.ca_state,
   current_date(),
   null,
    'Y'
    from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER c
    join SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS ca
    on c.c_current_addr_sk = ca.ca_address_sk;

    select count(*) from silver.customer_dim;

    select * from silver.customer_dim ;
    select * from silver.customer_dim limit 5;

create or replace table silver.customer_stage as 
select customer_id, state from silver.customer_dim where is_current ='Y';

update silver.customer_stage set state = 'TX' where customer_id=7991230;

MERGE INTO SILVER.CUSTOMER_DIM AS TARGET
    USING SILVER.CUSTOMER_STAGE AS SOURCE
    ON TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID
    AND TARGET.IS_CURRENT = 'Y'
    WHEN MATCHED AND TARGET.STATE <> SOURCE.STATE THEN
        UPDATE SET TARGET.END_DATE = CURRENT_DATE(), TARGET.IS_CURRENT = 'N'
    WHEN NOT MATCHED THEN
        INSERT (customer_id, state, start_date, end_date, is_current)
        VALUES (SOURCE.customer_id, SOURCE.state, CURRENT_DATE(), NULL, 'Y');

--validate SCD behaivour

select *
from silver.customer_dim 
where customer_id =7991230
order by start_date;