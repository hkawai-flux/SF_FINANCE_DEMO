{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_execution_lk') }}

with union_links as (
    -- 国内現物
    select
        {{ dbt_utils.generate_surrogate_key(['order_hk', 'execution_hk']) }} as order_execution_lk,
        order_hk,
        execution_hk,
        load_date,
        record_source
    from {{ ref('stg_stock_cash_executions') }}
    union
    -- 国内信用
    select
        {{ dbt_utils.generate_surrogate_key(['order_hk', 'execution_hk']) }} as order_execution_lk,
        order_hk,
        execution_hk,
        load_date,
        record_source
    from {{ ref('stg_stock_margin_executions') }}
    union
    -- 外国株
    select
        {{ dbt_utils.generate_surrogate_key(['order_hk', 'execution_hk']) }} as order_execution_lk,
        order_hk,
        execution_hk,
        load_date,
        record_source
    from {{ ref('stg_foreign_stock_executions') }}
)
select * from union_links
where 1=1
{% if is_incremental() %}
  and order_execution_lk not in (select order_execution_lk from {{ this }})
{% endif %}