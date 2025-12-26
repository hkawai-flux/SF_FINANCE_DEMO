{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['account_id', 'stock_symbol', 'as_of_date']) }} as holding_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['stock_symbol']) }} as stock_hk,
    {{ dbt_utils.generate_surrogate_key(['quantity', 'average_cost']) }} as holding_hashdiff,
    account_id,
    stock_symbol,
    quantity,
    average_cost,
    as_of_date,
    current_timestamp() as load_date,
    'FINANCE_RAW.STOCK_CASH_HOLDINGS' as record_source
from {{ source('finance_raw', 'stock_cash_holdings') }}