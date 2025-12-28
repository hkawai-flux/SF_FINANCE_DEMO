{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['account_hk', 'load_date']) }}

select
    account_hk,
    account_id,
    customer_hashdiff,
    customer_name,
    customer_type,
    segment,
    load_date,
    record_source
from {{ ref('stg_customer_profiles') }}
{% if is_incremental() %}
  where customer_hashdiff not in (select customer_hashdiff from {{ this }} where account_hk = account_hk)
{% endif %}