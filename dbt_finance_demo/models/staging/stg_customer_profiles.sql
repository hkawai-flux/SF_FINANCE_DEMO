{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['customer_name', 'customer_type', 'segment']) }} as customer_hashdiff,
    *,
    updated_at as event_at,
    current_timestamp() as load_date,
    'CRM_SYSTEM' as record_source
from {{ source('finance_raw', 'customer_profiles') }}