{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_hk') }}

with union_orders as (
    -- 国内現物注文
    select order_hk, order_id, load_date, record_source from {{ ref('stg_stock_cash_orders') }}
    union
    -- 国内信用注文
    select order_hk, order_id, load_date, record_source from {{ ref('stg_stock_margin_orders') }}
    union
    -- 外国株注文
    select order_hk, order_id, load_date, record_source from {{ ref('stg_foreign_stock_orders') }}
),
distinct_orders as (
    select
        order_hk,
        order_id,
        load_date,
        record_source,
        row_number() over (partition by order_hk order by load_date asc) as rnum
    from union_orders
)
select
    order_hk,
    order_id,
    load_date,
    record_source
from distinct_orders
where rnum = 1
{% if is_incremental() %}
  and order_hk not in (select order_hk from {{ this }})
{% endif %}