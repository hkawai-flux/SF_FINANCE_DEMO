{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['brand_cd']) }} as stock_hk,
    {{ dbt_utils.generate_surrogate_key(['brand_name', 'market_name', 'sector_name']) }} as stock_hashdiff,
    *,
    updated_at as event_at,
    current_timestamp() as load_date,
    'MASTER_SYSTEM' as record_source
from {{ source('finance_raw', 'stock_master') }}