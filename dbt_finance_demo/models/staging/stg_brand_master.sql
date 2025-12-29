{{ config(materialized='view') }}

select
    sha2_binary(brand_cd, 256) as stock_hk,
    
    sha2_binary(concat_ws('|', 
        coalesce(brand_name, ''),
        coalesce(market_name, ''),
        coalesce(sector_name, ''),
        coalesce(country_code, '')
    ), 256) as stock_hashdiff,

    brand_cd,
    brand_name,
    market_name,
    sector_name,
    country_code,
    is_active,
    updated_at,
    -- メタデータ
    current_timestamp() as load_date,
    'MASTER_DB' as record_source
from {{ source('finance_raw', 'stock_master') }}