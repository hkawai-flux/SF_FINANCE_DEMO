{{ config(materialized='view') }}

select
    sha2_binary(execution_id, 256) as execution_hk,
    sha2_binary(order_id, 256) as order_hk,
    sha2_binary(account_id, 256) as account_hk,
    sha2_binary(brand_cd, 256) as stock_hk,
    
    sha2_binary(concat_ws('|', 
        coalesce(cast(quantity as string), ''),
        coalesce(cast(price as string), ''),
        coalesce(currency_code, ''),
        coalesce(cast(execution_date as string), '')
    ), 256) as execution_hashdiff,

    execution_id,
    order_id,
    account_id,
    brand_cd,
    quantity,
    price,
    currency_code,
    execution_date,
    current_timestamp() as load_date,
    'SNOWFLAKE_RAW_FOREIGN' as record_source
from {{ source('finance_raw', 'foreign_stock_executions') }}