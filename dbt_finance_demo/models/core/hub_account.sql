{{ config(materialized='incremental', incremental_strategy='merge', unique_key='account_hk') }}

with union_accounts as (
    -- 各Stagingからaccount_hkとビジネスキーを抽出
    select account_hk, account_id, load_date, record_source from {{ ref('stg_customer_profiles') }}
    union
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_cash_executions') }}
    union
    select account_hk, account_id, load_date, record_source from {{ ref('stg_foreign_stock_executions') }}
    union
    select account_hk, account_id, load_date, record_source from {{ ref('stg_cash_transactions') }}
),
distinct_accounts as (
    -- ビジネスキーごとに最も古い（最初の）レコードを採用
    select
        account_hk,
        account_id,
        load_date,
        record_source,
        row_number() over (partition by account_hk order by load_date asc) as rnum
    from union_accounts
)
select
    account_hk,
    account_id,
    load_date,
    record_source
from distinct_accounts
where rnum = 1
{% if is_incremental() %}
  and account_hk not in (select account_hk from {{ this }})
{% endif %}