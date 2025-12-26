{{ config(materialized='incremental', unique_key='order_link_hk') }}

with all_order_links as (
    -- 現物注文から関係を抽出
    select
        {{ dbt_utils.generate_surrogate_key(['order_hk', 'account_hk', 'stock_hk']) }} as order_link_hk,
        order_hk,
        account_hk,
        stock_hk,
        load_date,
        record_source
    from {{ ref('stg_stock_cash_orders') }}
    
    union all

    -- 信用注文から関係を抽出
    select
        {{ dbt_utils.generate_surrogate_key(['order_hk', 'account_hk', 'stock_hk']) }} as order_link_hk,
        order_hk,
        account_hk,
        stock_hk,
        load_date,
        record_source
    from {{ ref('stg_stock_margin_orders') }}
),

distinct_links as (
    select
        order_link_hk,
        order_hk,
        account_hk,
        stock_hk,
        min(load_date) as load_date,
        min(record_source) as record_source
    from all_order_links
    group by 1, 2, 3, 4
)

select * from distinct_links
{% if is_incremental() %}
    where order_link_hk not in (select order_link_hk from {{ this }})
{% endif %}