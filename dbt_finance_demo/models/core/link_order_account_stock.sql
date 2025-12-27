{{ config(materialized='incremental', unique_key='order_account_stock_lk') }}

select
    {{ dbt_utils.generate_surrogate_key(['order_hk', 'account_hk', 'stock_hk']) }} as order_account_stock_lk, -- ここをこの名前に固定
    order_hk,
    account_hk,
    stock_hk,
    load_date,
    record_source
from {{ ref('stg_stock_cash_orders') }} -- 実際にはunion all等が入ります