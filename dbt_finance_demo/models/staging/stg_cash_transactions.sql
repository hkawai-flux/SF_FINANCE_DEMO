{{ config(materialized='view') }}

select
    sha2_binary(transaction_id, 256) as transaction_hk,
    sha2_binary(account_id, 256) as account_hk,
    
    sha2_binary(concat_ws('|', 
        coalesce(transaction_type, ''),
        coalesce(cast(amount as string), ''),
        coalesce(currency_code, ''),
        coalesce(cast(executed_at as string), '')
    ), 256) as transaction_hashdiff,

    transaction_id,
    account_id,
    transaction_type,
    amount,
    currency_code,
    executed_at,
    current_timestamp() as load_date,
    'CASH_MANAGEMENT_SYSTEM' as record_source
from {{ source('finance_raw', 'cash_transactions') }}