{{ config(materialized='incremental', incremental_strategy='merge', unique_key='brand_hk') }}

with union_brands as (
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_brand_master') }}
    union
    -- 国内株/現物取引関連のStagingからbrand_hkとビジネスキーを抽出
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_stock_cash_orders') }}
    union
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_stock_cash_holdings') }}
    union
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_stock_cash_executions') }}
    union
    -- 国内株/信用取引関連のStagingからbrand_hkとビジネスキーを抽出
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_stock_margin_orders') }}
    union
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_stock_margin_holdings') }}
    union
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_stock_margin_executions') }}
    union
    -- 外国株 取引関連のStagingからbrand_hkとビジネスキーを抽出
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_foreign_stock_orders') }}
    union
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_foreign_stock_holdings') }}
    union
    select brand_hk, brand_cd, load_date, record_source from {{ ref('stg_foreign_stock_executions') }}
),
distinct_brands as (
    select
        brand_hk,
        brand_cd,
        load_date,
        record_source,
        row_number() over (partition by brand_hk order by load_date asc) as rnum
    from union_brands
)
select
    brand_hk,
    brand_cd,
    load_date,
    record_source
from distinct_brands
where rnum = 1
{% if is_incremental() %}
  and brand_hk not in (select brand_hk from {{ this }})
{% endif %}