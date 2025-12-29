{{ config(materialized='incremental', unique_key='order_account_brand_lk') }}

with union_links as (
select
    {{ dbt_utils.generate_surrogate_key(['order_hk', 'account_hk', 'brand_hk']) }} as order_account_brand_lk,
    order_hk,
    account_hk,
    brand_hk,
    load_date,
    record_source
from {{ ref('stg_stock_cash_orders') }}
UNION
select {{ dbt_utils.generate_surrogate_key(['order_hk', 'account_hk', 'brand_hk']) }}, order_hk, account_hk, brand_hk, load_date, record_source
from {{ ref('stg_stock_margin_orders') }}
union
select {{ dbt_utils.generate_surrogate_key(['order_hk', 'account_hk', 'brand_hk']) }}, order_hk, account_hk, brand_hk, load_date, record_source
from {{ ref('stg_foreign_stock_orders') }}
)
select * from union_links
where 1=1
{% if is_incremental() %}
  and order_account_brand_lk not in (select order_account_brand_lk from {{ this }})
{% endif %} 