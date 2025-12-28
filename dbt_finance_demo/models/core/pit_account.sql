{{ config(materialized='table') }}

with as_of_dates as (
    select cast('2025-12-28' as date) as as_of_date
    union all
    select current_date()
),
account_hubs as (
    select account_hk from {{ ref('hub_account') }}
),
base as (
    select
        h.account_hk,
        d.as_of_date
    from account_hubs h
    cross join as_of_dates d
),
sat_cust as (
    select 
        s.account_hk,
        s.load_date,
        b.as_of_date
    from base b
    inner join {{ ref('sat_customer_details') }} s 
        on b.account_hk = s.account_hk 
        and s.load_date <= b.as_of_date
    qualify row_number() over (partition by b.account_hk, b.as_of_date order by s.load_date desc) = 1
),
sat_hold as (
    -- 現物保有も同様に特定
    select 
        s.account_hk,
        s.load_date,
        b.as_of_date
    from base b
    left join {{ ref('sat_stock_cash_holdings') }} s 
        on b.account_hk = s.account_hk 
        and s.load_date <= b.as_of_date
    qualify row_number() over (partition by b.account_hk, b.as_of_date order by s.load_date desc) = 1
)
select
    sha2_binary(concat(to_varchar(b.account_hk), to_varchar(b.as_of_date)), 256) as pit_account_hk,
    b.account_hk,
    b.as_of_date,
    c.load_date as customer_load_date,
    h.load_date as cash_holding_load_date,
    current_timestamp() as load_date
from base b
left join sat_cust c 
    on b.account_hk = c.account_hk and b.as_of_date = c.as_of_date
left join sat_hold h 
    on b.account_hk = h.account_hk and b.as_of_date = h.as_of_date