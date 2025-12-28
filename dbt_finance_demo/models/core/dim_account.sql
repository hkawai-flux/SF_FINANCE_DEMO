{{ config(
    materialized='dynamic_table',
    target_lag='downstream',
    refresh_mode='auto',
    initialize='on_create',
    snowflake_warehouse='FINANCE_DEMO_WH'
) }}

select
    p.account_hk,
    c.account_id,
    c.customer_name,
    c.segment,
    c.load_date
from {{ ref('pit_account') }} p
join {{ ref('sat_customer_details') }} c
    on p.account_hk = c.account_hk 
    --and p.customer_load_date = c.load_date
group by 1,2,3,4,5
