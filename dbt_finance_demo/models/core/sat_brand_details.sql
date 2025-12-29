{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['brand_hk', 'load_date']) }}

select
    brand_hk,
    brand_hashdiff,
    brand_name,
    market_name,
    sector_name,
    country_code,
    is_active,
    load_date,
    record_source
from {{ ref('stg_brand_master') }}
{% if is_incremental() %}
  where brand_hashdiff not in (select brand_hashdiff from {{ this }} where brand_hk = brand_hk)
{% endif %}