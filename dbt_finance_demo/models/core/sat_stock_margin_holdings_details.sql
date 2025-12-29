{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['holding_hk', 'load_date']) }}

select
    holding_hk,
    account_hk,
    holding_hashdiff,
    base_date
    brand_cd,
    quantity,
    open_price,
    current_price,
    side,
    load_date,
    record_source
from {{ ref('stg_stock_margin_holdings') }}
{% if is_incremental() %}
  where holding_hashdiff not in (select holding_hashdiff from {{ this }} where holding_hk = holding_hk)
{% endif %}