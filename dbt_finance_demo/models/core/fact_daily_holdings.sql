{{ config(
    materialized='dynamic_table',
    target_lag='downstream',
    snowflake_warehouse='FINANCE_DEMO_WH',
) }}

select
    p.pit_account_hk,
    p.account_hk,
    p.base_date,
    s.brand_cd,
    s.quantity,
    case when s.quantity > 0 then 1 else 0 end as has_stock_balance
from {{ ref('pit_account') }} p
join {{ ref('sat_stock_cash_holdings_details') }} s
    on p.account_hk = s.account_hk 
union
select
    q.pit_account_hk,
    q.account_hk,
    q.base_date,
    u.brand_cd,
    u.quantity,
    case when u.quantity > 0 then 1 else 0 end as has_stock_balance
from {{ ref('pit_account') }} q
join {{ ref('sat_stock_margin_holdings_details') }} u
    on q.account_hk = u.account_hk 