{{ config(materialized='incremental', unique_key='account_hk') }}

with all_accounts as (
    -- 現物注文から抽出
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_cash_orders') }}
    union all
    -- 信用注文から抽出
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_margin_orders') }}
    union all
    -- 現物預りから抽出
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_cash_holdings') }}
    union all
    -- 信用預りから抽出
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_margin_holdings') }}
),

distinct_accounts as (
    select
        account_hk,
        account_id,
        min(load_date) as load_date,
        min(record_source) as record_source
    from all_accounts
    group by 1, 2
)

select * from distinct_accounts
{% if is_incremental() %}
    where account_hk not in (select account_hk from {{ this }})
{% endif %}