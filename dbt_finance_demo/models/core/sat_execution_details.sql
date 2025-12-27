{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['execution_hk', 'load_date']) }}

with union_details as (
    select execution_hk, execution_hashdiff, quantity, price, 'JPY' as currency_code, load_date, record_source from {{ ref('stg_stock_cash_executions') }}
    union all
    select execution_hk, execution_hashdiff, quantity, price, 'JPY' as currency_code, load_date, record_source from {{ ref('stg_stock_margin_executions') }}
    union all
    select execution_hk, execution_hashdiff, quantity, price, currency_code, load_date, record_source from {{ ref('stg_foreign_stock_executions') }}
)
select * from union_details
{% if is_incremental() %}
  where execution_hashdiff not in (select execution_hashdiff from {{ this }} where execution_hk = execution_hk)
{% endif %}