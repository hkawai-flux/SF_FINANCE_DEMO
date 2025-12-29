{{ config(materialized='view') }}

select
    sha2_binary(concat(account_id, brand_cd), 256) as holding_hk,
    sha2_binary(account_id, 256) as account_hk,
    sha2_binary(brand_cd, 256) as brand_hk,
    
    sha2_binary(concat_ws('|', 
        coalesce(cast(quantity as string), ''),
        coalesce(side, '')
    ), 256) as holding_hashdiff,

    as_of_date as base_date,
    account_id,
    brand_cd,
    quantity,
    open_price,
    current_price,
    side,
    evaluation_profit_loss,
    -- メタデータ
    current_timestamp() as load_date,
    'SNOWFLAKE_RAW_MARGIN' as record_source
from {{ source('finance_raw', 'stock_margin_holdings') }}