{{ config(materialized='view') }}

select
    sha2_binary(execution_id, 256) as execution_hk,
    sha2_binary(order_id, 256) as order_hk,
    sha2_binary(account_id, 256) as account_hk,
    sha2_binary(brand_cd, 256) as brand_hk,
    
    sha2_binary(concat_ws('|', 
        coalesce(cast(quantity as string), ''),
        coalesce(cast(price as string), ''),
        coalesce(trade_type, ''),
        coalesce(margin_type, '')
    ), 256) as execution_hashdiff,

    execution_date as base_date,
    execution_id,
    order_id,
    account_id,
    brand_cd,
    quantity,
    price,
    trade_type,
    margin_type,
    interest_rate,
    executed_at,
    -- メタデータ
    current_timestamp() as load_date,
    'SNOWFLAKE_RAW_MARGIN' as record_source
from {{ source('finance_raw', 'stock_margin_executions') }}