{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['account_id', 'brand_cd', 'as_of_date']) }} as holding_hk,
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_hk,
    {{ dbt_utils.generate_surrogate_key(['brand_cd']) }} as stock_hk,
    {{ dbt_utils.generate_surrogate_key(['quantity', 'average_cost', 'currency_code']) }} as holding_hashdiff,
    *,
    as_of_date as event_at,
    current_timestamp() as load_date,
    'BACKOFFICE_FOREIGN' as record_source
from {{ source('finance_raw', 'foreign_stock_holdings') }}