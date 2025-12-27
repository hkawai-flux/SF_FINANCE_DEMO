{{ config(materialized='incremental', unique_key='order_hk') }}

select
    order_hk,
    order_hashdiff,
    order_quantity,
    order_price,
    order_status,
    event_at,
    load_date,
    record_source
from {{ ref('stg_stock_cash_orders') }}

{% if is_incremental() %}
    where order_hashdiff not in (
        select order_hashdiff from {{ this }}
    )
{% endif %}

union all

select
    order_hk,
    order_hashdiff,
    order_quantity,
    order_price,
    'COMPLETED' as order_status, -- 信用は簡易的に固定
    event_at,
    load_date,
    record_source
from {{ ref('stg_stock_margin_orders') }}

{% if is_incremental() %}
    where order_hashdiff not in (
        select order_hashdiff from {{ this }}
    )
{% endif %}