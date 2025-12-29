{{ config(materialized='incremental', incremental_strategy='merge', unique_key='account_brand_holding_lk') }}

with union_links as (
    select
        {{ dbt_utils.generate_surrogate_key(['account_hk', 'brand_hk']) }} as account_brand_holding_lk,
        account_hk,
        brand_hk,
        load_date,
        record_source
    from {{ ref('stg_stock_cash_holdings') }}
    union
    select {{ dbt_utils.generate_surrogate_key(['account_hk', 'brand_hk']) }}, account_hk, brand_hk, load_date, record_source
    from {{ ref('stg_stock_margin_holdings') }}
    union
    select {{ dbt_utils.generate_surrogate_key(['account_hk', 'brand_hk']) }}, account_hk, brand_hk, load_date, record_source
    from {{ ref('stg_foreign_stock_holdings') }}
)
select * from union_links
where 1=1
{% if is_incremental() %}
  and account_brand_holding_lk not in (select account_brand_holding_lk from {{ this }})
{% endif %}