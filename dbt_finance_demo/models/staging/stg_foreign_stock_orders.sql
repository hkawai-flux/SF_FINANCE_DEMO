{{ config(materialized='view') }}

select
    -- ビジネスキーを SHA2 で BINARY(32) 化
    sha2_binary(order_id, 256) as order_hk,
    sha2_binary(account_id, 256) as account_hk,
    sha2_binary(brand_cd, 256) as brand_hk,
    
    -- 属性変更検知用の HashDiff
    sha2_binary(concat_ws('|', 
        coalesce(market_code, ''),
        coalesce(cast(order_quantity as string), ''),
        coalesce(currency_code, ''),
        coalesce(cast(ordered_at as string), '')
    ), 256) as order_hashdiff,

    -- ビジネスキーと属性
    cast(ordered_at as date) as base_date,
    order_id,
    account_id,
    brand_cd,
    market_code,
    currency_code,
    order_quantity,
    order_price,
    ordered_at,    
    -- メタデータ
    current_timestamp() as load_date,
    'SNOWFLAKE_RAW_FOREIGN' as record_source
from {{ source('finance_raw', 'foreign_stock_orders') }}