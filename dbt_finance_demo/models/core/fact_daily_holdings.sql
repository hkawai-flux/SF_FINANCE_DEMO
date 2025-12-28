{{ config(
    materialized='dynamic_table',
    target_lag='downstream',
    snowflake_warehouse='FINANCE_DEMO_WH',
) }}

select
    p.pit_account_hk,
    p.account_hk,
    p.as_of_date,
    s.brand_cd,
    s.quantity,
    case when s.quantity > 0 then 1 else 0 end as has_stock_balance
from {{ ref('pit_account') }} p
join {{ ref('sat_stock_cash_holdings') }} s
    on p.account_hk = s.account_hk 
    --and p.cash_holding_load_date = s.load_date