{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['holding_hk', 'load_date']) }}

select
    holding_hk,
    holding_hashdiff,
    quantity,
    average_cost,
    as_of_date,
    load_date,
    record_source
from {{ ref('stg_stock_cash_holdings') }}
{% if is_incremental() %}
  where holding_hashdiff not in (select holding_hashdiff from {{ this }} where holding_hk = holding_hk)
{% endif %}