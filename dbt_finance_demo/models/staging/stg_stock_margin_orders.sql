{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['stock_symbol']) }} as stock_hk,
    {{ dbt_utils.generate_surrogate_key(['order_type', 'margin_type', 'order_quantity', 'order_price']) }} as order_hashdiff,
    order_id,
    account_id,
    stock_symbol,
    order_type,
    margin_type,
    order_quantity,
    order_price,
    ordered_at as event_at,
    current_timestamp() as load_date,
    'FINANCE_RAW.STOCK_MARGIN_ORDERS' as record_source
from {{ source('finance_raw', 'stock_margin_orders') }}