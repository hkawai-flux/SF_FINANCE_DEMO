{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['execution_id']) }} as execution_hk,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['brand_cd']) }} as stock_hk,
    {{ dbt_utils.generate_surrogate_key(['quantity', 'price', 'commission']) }} as execution_hashdiff,
    *,
    executed_at as event_at,
    current_timestamp() as load_date,
    'TRADING_SYSTEM_CASH' as record_source
from {{ source('finance_raw', 'stock_cash_executions') }}