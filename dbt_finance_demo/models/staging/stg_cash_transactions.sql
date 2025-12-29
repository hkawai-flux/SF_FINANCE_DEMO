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

    execution_date as base_date,
    transaction_id,
    account_id,
    currency_code,
    transaction_type,
    amount,
    description,
    executed_at,
    -- メタデータ
    current_timestamp() as load_date,
    'CASH_MANAGEMENT_SYSTEM' as record_source
from {{ source('finance_raw', 'cash_transactions') }}