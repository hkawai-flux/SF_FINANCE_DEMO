{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['account_id', 'stock_symbol', 'side', 'as_of_date']) }} as holding_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['stock_symbol']) }} as stock_hk,
    {{ dbt_utils.generate_surrogate_key(['quantity', 'open_price', 'current_price', 'evaluation_profit_loss']) }} as holding_hashdiff,
    account_id,
    stock_symbol,
    side,
    quantity,
    open_price,
    current_price,
    evaluation_profit_loss,
    as_of_date,
    current_timestamp() as load_date,
    'FINANCE_RAW.STOCK_MARGIN_HOLDINGS' as record_source
from {{ source('finance_raw', 'stock_margin_holdings') }}