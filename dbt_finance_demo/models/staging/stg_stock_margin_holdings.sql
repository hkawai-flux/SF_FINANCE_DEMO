{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['account_id', 'brand_cd', 'side', 'as_of_date']) }} as holding_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['brand_cd']) }} as stock_hk,
    {{ dbt_utils.generate_surrogate_key(['quantity', 'open_price', 'current_price', 'evaluation_profit_loss']) }} as holding_hashdiff,
    *,
    as_of_date as event_at,
    current_timestamp() as load_date,
    'BACKOFFICE_MARGIN' as record_source
from {{ source('finance_raw', 'stock_margin_holdings') }}