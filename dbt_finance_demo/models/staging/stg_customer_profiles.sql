{{ config(materialized='view') }}

select
    sha2_binary(account_id, 256) as account_hk,
    
    sha2_binary(concat_ws('|', 
        coalesce(customer_name, ''),
        coalesce(customer_type, ''),
        coalesce(segment, '')
    ), 256) as customer_hashdiff,

    account_id,
    customer_name,
    customer_type,
    segment,
    current_timestamp() as load_date,
    'CRM_SYSTEM' as record_source
from {{ source('finance_raw', 'customer_profiles') }}