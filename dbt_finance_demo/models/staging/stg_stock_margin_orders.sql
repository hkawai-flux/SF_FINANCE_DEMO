{{ config(materialized='view') }}

select
    sha2_binary(order_id, 256) as order_hk,
    sha2_binary(account_id, 256) as account_hk,
    sha2_binary(brand_cd, 256) as stock_hk,
    
    sha2_binary(concat_ws('|', 
        coalesce(order_type, ''), -- 新規/返済
        coalesce(margin_type, ''), -- 新規/返済
        coalesce(cast(ordered_at as string), '')
    ), 256) as order_hashdiff,

    order_id,
    account_id,
    brand_cd,
    order_type,
    margin_type,
    ordered_at,
    current_timestamp() as load_date,
    'SNOWFLAKE_RAW_MARGIN' as record_source
from {{ source('finance_raw', 'stock_margin_orders') }}