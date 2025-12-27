{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='pit_account_hk'
) }}

with as_of_dates as (
    -- 基準日（Snapshot日）のリストを生成（全日または月末など）
    select distinct as_of_date from {{ ref('stg_stock_cash_holdings') }}
),
account_hubs as (
    select account_hk from {{ ref('hub_account') }}
),
base as (
    -- 口座 × 基準日のマトリックスを作成
    select
        {{ dbt_utils.generate_surrogate_key(['h.account_hk', 'd.as_of_date']) }} as pit_account_hk,
        h.account_hk,
        d.as_of_date
    from account_hubs h
    cross join as_of_dates d
),
pit as (
    select
        b.pit_account_hk,
        b.account_hk,
        b.as_of_date,
        -- 顧客属性サテライトの有効なload_dateを特定
        (select max(load_date) from {{ ref('sat_customer_details') }} s 
         where s.account_hk = b.account_hk and s.load_date <= b.as_of_date) as customer_load_date,
        -- 現物残高サテライトの有効なload_dateを特定
        (select max(load_date) from {{ ref('sat_stock_cash_holdings') }} s 
         where s.holding_hk in (select holding_hk from {{ ref('stg_stock_cash_holdings') }} where account_hk = b.account_hk) 
         and s.load_date <= b.as_of_date) as cash_holding_load_date
    from base b
)
select 
    *,
    current_timestamp() as load_date
from pit
where 1=1
{% if is_incremental() %}
  and pit_account_hk not in (select pit_account_hk from {{ this }})
{% endif %}