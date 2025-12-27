{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['stock_hk', 'load_date']) }}

select
    stock_hk,
    stock_hashdiff,
    brand_name,
    market_name,
    sector_name,
    country_code,
    is_active,
    load_date,
    record_source
from {{ ref('stg_stock_master') }}
{% if is_incremental() %}
  where stock_hashdiff not in (select stock_hashdiff from {{ this }} where stock_hk = stock_hk)
{% endif %}