{{ config(materialized='view') }}

select
    -- ビジネスキーをSHA2でBINARY(32)化
    sha2_binary(execution_id, 256) as execution_hk,
    sha2_binary(order_id, 256) as order_hk,
    sha2_binary(account_id, 256) as account_hk,
    sha2_binary(brand_cd, 256) as brand_hk,
    
    -- 属性変更検知用のHashDiff
    sha2_binary(concat_ws('|', 
        coalesce(cast(quantity as string), ''),
        coalesce(cast(price as string), ''),
        coalesce(cast(execution_date as string), '')
    ), 256) as execution_hashdiff,

    -- ビジネスキーと属性
    execution_date as base_date,
    execution_id,
    order_id,
    account_id,
    brand_cd,
    trade_type,
    quantity,
    price,
    commission,    
    executed_at,
    -- メタデータ
    current_timestamp() as load_date,
    'SNOWFLAKE_RAW_CASH' as record_source
from {{ source('finance_raw', 'stock_cash_executions') }}