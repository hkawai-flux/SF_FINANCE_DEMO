{{ config(materialized='incremental', unique_key='holding_hk') }}

select
    holding_hk,
    account_hk,
    holding_hashdiff,
    side,
    quantity,
    open_price,
    current_price,
    evaluation_profit_loss,
    as_of_date,
    load_date,
    record_source
from {{ ref('stg_stock_margin_holdings') }}

{% if is_incremental() %}
    where holding_hashdiff not in (
        select holding_hashdiff from {{ this }}
    )
{% endif %}