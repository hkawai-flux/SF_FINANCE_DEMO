{{ config(materialized='incremental', incremental_strategy='merge', unique_key='stock_hk') }}

with union_stocks as (
    select stock_hk, brand_cd, load_date, record_source from {{ ref('stg_stock_master') }}
    union
    select stock_hk, brand_cd, load_date, record_source from {{ ref('stg_stock_cash_executions') }}
    union
    select stock_hk, brand_cd, load_date, record_source from {{ ref('stg_foreign_stock_executions') }}
),
distinct_stocks as (
    select
        stock_hk,
        brand_cd,
        load_date,
        record_source,
        row_number() over (partition by stock_hk order by load_date asc) as rnum
    from union_stocks
)
select
    stock_hk,
    brand_cd,
    load_date,
    record_source
from distinct_stocks
where rnum = 1
{% if is_incremental() %}
  and stock_hk not in (select stock_hk from {{ this }})
{% endif %}