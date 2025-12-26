{{ config(materialized='incremental', unique_key='stock_hk') }}

with all_stocks as (
    select stock_hk, stock_symbol, load_date, record_source from {{ ref('stg_stock_cash_orders') }}
    union all
    select stock_hk, stock_symbol, load_date, record_source from {{ ref('stg_stock_margin_orders') }}
    union all
    select stock_hk, stock_symbol, load_date, record_source from {{ ref('stg_stock_cash_holdings') }}
    union all
    select stock_hk, stock_symbol, load_date, record_source from {{ ref('stg_stock_margin_holdings') }}
),

distinct_stocks as (
    select
        stock_hk,
        stock_symbol,
        min(load_date) as load_date,
        min(record_source) as record_source
    from all_stocks
    group by 1, 2
)

select * from distinct_stocks
{% if is_incremental() %}
    where stock_hk not in (select stock_hk from {{ this }})
{% endif %}