{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as transaction_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['amount', 'transaction_type', 'currency_code']) }} as transaction_hashdiff,
    *,
    executed_at as event_at,
    current_timestamp() as load_date,
    'CASH_SYSTEM' as record_source
from {{ source('finance_raw', 'cash_transactions') }}