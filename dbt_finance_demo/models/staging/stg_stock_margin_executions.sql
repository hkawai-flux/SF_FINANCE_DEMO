{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['execution_id']) }} as execution_hk,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_hk,
    execution_id,
    order_id,
    trade_type,
    margin_type,
    quantity,
    price,
    interest_rate,
    executed_at as event_at,
    current_timestamp() as load_date,
    'FINANCE_RAW.STOCK_MARGIN_EXECUTIONS' as record_source
from {{ source('finance_raw', 'stock_margin_executions') }}