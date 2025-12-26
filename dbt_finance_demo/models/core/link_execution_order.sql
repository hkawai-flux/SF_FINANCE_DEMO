{{ config(materialized='incremental', unique_key='execution_link_hk') }}

with all_execution_links as (
    -- 現物約定から関係を抽出
    select
        {{ dbt_utils.generate_surrogate_key(['execution_hk', 'order_hk']) }} as execution_link_hk,
        execution_hk,
        order_hk,
        load_date,
        record_source
    from {{ ref('stg_stock_cash_executions') }}
    
    union all

    -- 信用約定から関係を抽出
    select
        {{ dbt_utils.generate_surrogate_key(['execution_hk', 'order_hk']) }} as execution_link_hk,
        execution_hk,
        order_hk,
        load_date,
        record_source
    from {{ ref('stg_stock_margin_executions') }}
),

distinct_links as (
    select
        execution_link_hk,
        execution_hk,
        order_hk,
        min(load_date) as load_date,
        min(record_source) as record_source
    from all_execution_links
    group by 1, 2, 3
)

select * from distinct_links
{% if is_incremental() %}
    where execution_link_hk not in (select execution_link_hk from {{ this }})
{% endif %}