{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['brand_cd']) }} as stock_hk,
    {{ dbt_utils.generate_surrogate_key(['order_quantity', 'order_price', 'margin_type']) }} as order_hashdiff,
    *,
    ordered_at as event_at,
    current_timestamp() as load_date,
    'TRADING_SYSTEM_MARGIN' as record_source
from {{ source('finance_raw', 'stock_margin_orders') }}