{{ config(materialized='incremental', incremental_strategy='merge', unique_key='account_hk') }}

with union_accounts as (
    -- 顧客プロファイル関連のStagingからaccount_hkとビジネスキーを抽出
    select account_hk, account_id, load_date, record_source from {{ ref('stg_customer_profiles') }}
    union
    -- 国内株/現物取引関連のStagingからaccount_hkとビジネスキーを抽出
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_cash_orders') }}
    union
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_cash_holdings') }}
    union
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_cash_executions') }}
    union
    -- 国内株/信用取引関連のStagingからaccount_hkとビジネスキーを抽出
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_margin_orders') }}
    union
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_margin_holdings') }}
    union
    select account_hk, account_id, load_date, record_source from {{ ref('stg_stock_margin_executions') }}
    union
    -- 外国株 取引関連のStagingからaccount_hkとビジネスキーを抽出
    select account_hk, account_id, load_date, record_source from {{ ref('stg_foreign_stock_orders') }}
    union
    select account_hk, account_id, load_date, record_source from {{ ref('stg_foreign_stock_holdings') }}
    union
    select account_hk, account_id, load_date, record_source from {{ ref('stg_foreign_stock_executions') }}
    union
    -- キャッシュ取引関連のStagingからaccount_hkとビジネスキーを抽出
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