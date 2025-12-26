{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['execution_id']) }} as execution_hk,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['stock_symbol']) }} as stock_hk,
    execution_id,
    order_id,
    trade_type,
    quantity,
    price,
    commission,
    executed_at as event_at,
    current_timestamp() as load_date,
    'FINANCE_RAW.STOCK_CASH_EXECUTIONS' as record_source
from {{ source('finance_raw', 'stock_cash_executions') }}