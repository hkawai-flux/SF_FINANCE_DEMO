{{ config(materialized='incremental', incremental_strategy='merge', unique_key='account_transaction_lk') }}

select
    {{ dbt_utils.generate_surrogate_key(['account_hk', 'transaction_hk']) }} as account_transaction_lk,
    account_hk,
    transaction_hk,
    load_date,
    record_source
from {{ ref('stg_cash_transactions') }}
where 1=1
{% if is_incremental() %}
  and account_transaction_lk not in (select account_transaction_lk from {{ this }})
{% endif %}