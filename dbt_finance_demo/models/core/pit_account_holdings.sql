{{ config(
    materialized='incremental',
    unique_key='pit_hk'
) }}

-- 1. 時間軸（Time Spine）の取得
with dates as (
    select date_day
    from {{ ref('all_days') }}
    where date_day <= current_date()
    {% if is_incremental() %}
      and date_day > (select max(snapshot_date) from {{ this }})
    {% endif %}
),

-- 2. 対象となる Hub の取得
hubs as (
    select account_hk, account_id from {{ ref('hub_account') }}
),

-- 3. 特定時点における最新の Satellite レコードを特定
active_sat_cash as (
    select
        h.account_hk,
        d.date_day as snapshot_date,
        max(s.load_date) as cash_holding_load_date
    from hubs h
    inner join dates d on 1=1
    left join {{ ref('sat_stock_cash_holding_details') }} s
        on h.account_hk = s.account_hk
        -- 修正：snapshot_dateの日付の範囲内（当日00:00〜翌日00:00未満）にあるデータを取得
        and s.load_date >= d.date_day 
        and s.load_date < dateadd(day, 1, d.date_day)
    group by 1, 2
),

active_sat_margin as (
    select
        h.account_hk,
        d.date_day as snapshot_date,
        max(s.load_date) as margin_holding_load_date
    from hubs h
    inner join dates d on 1=1
    left join {{ ref('sat_stock_margin_holding_details') }} s
        on h.account_hk = s.account_hk
        and s.load_date >= d.date_day 
        and s.load_date < dateadd(day, 1, d.date_day)
    group by 1, 2
)

-- 4. インデックスとして統合
select
    {{ dbt_utils.generate_surrogate_key(['c.account_hk', 'c.snapshot_date']) }} as pit_hk,
    c.account_hk,
    c.snapshot_date,
    c.cash_holding_load_date,
    m.margin_holding_load_date
from active_sat_cash c
left join active_sat_margin m
    on c.account_hk = m.account_hk
    and c.snapshot_date = m.snapshot_date