USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE SCHEMA IF NOT EXISTS gold;

create or replace table gold.sales_by_state as
select
     d.state,
     sum(f.sales_price) as total_sales
from silver.fact_sales f
join silver.customer_dim d on f.customer_sk = d.customer_sk
group by d.state;

select * from silver.customer_dim;

select * from silver.fact_sales;

create or replace table gold.sales_by_year as
select 
year(transaction_date) as sales_year,
sum(sales_price) as total_sales
from silver.fact_sales
group by sales_year;

create or replace table gold.monthly_sales as
select 
date_trunc('month',transaction_date) as sales_month,
sum(sales_price) as total_sales
from silver.fact_sales
group by sales_month;

create or replace table gold.top_stores as 
select 
store_id,
sum(sales_price) as total_sales
from 
silver.fact_sales
group by store_id
order by total_sales desc
limit 10;