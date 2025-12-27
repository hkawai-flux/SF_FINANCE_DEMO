{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['execution_id']) }} as execution_hk,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['brand_cd']) }} as stock_hk,
    {{ dbt_utils.generate_surrogate_key(['quantity', 'price', 'currency_code']) }} as execution_hashdiff,
    *,
    executed_at as event_at,
    current_timestamp() as load_date,
    'TRADING_SYSTEM_FOREIGN' as record_source
from {{ source('finance_raw', 'foreign_stock_executions') }}