{{ config(materialized='view') }}

select
    sha2_binary(order_id, 256) as order_hk,
    sha2_binary(account_id, 256) as account_hk,
    sha2_binary(brand_cd, 256) as stock_hk,
    
    sha2_binary(concat_ws('|', 
        coalesce(order_type, ''),
        coalesce(cast(order_quantity as string), ''),
        coalesce(order_status, '')
    ), 256) as order_hashdiff,

    order_id,
    account_id,
    brand_cd,
    order_type,
    order_quantity,
    order_status,
    ordered_at,
    current_timestamp() as load_date,
    'SNOWFLAKE_RAW_CASH' as record_source
from {{ source('finance_raw', 'stock_cash_orders') }}