{{ config(materialized='view') }}

select
    -- DV2.0 ハッシュキー
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['stock_symbol']) }} as stock_hk,
    -- ハッシュ差分（属性変更検知用）
    {{ dbt_utils.generate_surrogate_key(['order_type', 'order_quantity', 'order_price', 'order_status']) }} as order_hashdiff,
    -- オリジナルカラム
    order_id,
    account_id,
    stock_symbol,
    order_type,
    order_quantity,
    order_price,
    order_status,
    ordered_at as event_at,
    -- メタデータ
    current_timestamp() as load_date,
    'FINANCE_RAW.STOCK_CASH_ORDERS' as record_source
from {{ source('finance_raw', 'stock_cash_orders') }}