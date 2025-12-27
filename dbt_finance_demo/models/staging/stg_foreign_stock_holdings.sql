{{ config(materialized='view') }}

select
    sha2_binary(concat(account_id, brand_cd), 256) as holding_hk,
    sha2_binary(account_id, 256) as account_hk,
    sha2_binary(brand_cd, 256) as stock_hk,
    
    sha2_binary(concat_ws('|', 
        coalesce(cast(quantity as string), ''),
        coalesce(cast(average_cost as string), ''),
        coalesce(currency_code, '')
    ), 256) as holding_hashdiff,

    account_id,
    brand_cd,
    quantity,
    average_cost,
    currency_code,
    as_of_date,
    current_timestamp() as load_date,
    'SNOWFLAKE_RAW_FOREIGN' as record_source
from {{ source('finance_raw', 'foreign_stock_holdings') }}