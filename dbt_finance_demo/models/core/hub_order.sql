{{ config(materialized='incremental', unique_key='order_hk') }}

with all_orders as (
    -- 現物注文からビジネスキーを抽出
    select 
        order_hk, 
        order_id, 
        load_date, 
        record_source 
    from {{ ref('stg_stock_cash_orders') }}
    
    union all
    
    -- 信用注文からビジネスキーを抽出
    select 
        order_hk, 
        order_id, 
        load_date, 
        record_source 
    from {{ ref('stg_stock_margin_orders') }}
),

distinct_orders as (
    select
        order_hk,
        order_id,
        min(load_date) as load_date,
        min(record_source) as record_source
    from all_orders
    group by 1, 2
)

select * from distinct_orders
{% if is_incremental() %}
    where order_hk not in (select order_hk from {{ this }})
{% endif %}