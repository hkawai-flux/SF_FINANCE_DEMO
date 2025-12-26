{{ config(
    materialized='table',
    schema='semantic'
) }}

with pit as (
    -- PITテーブルから特定時点のインデックスを取得
    select * from {{ ref('pit_account_holdings') }}
),

hub_acc as (
    select account_hk, account_id from {{ ref('hub_account') }}
),

sat_cash as (
    -- 現物残高の属性
    select * from {{ ref('sat_stock_cash_holding_details') }}
),

sat_margin as (
    -- 信用残高の属性
    select * from {{ ref('sat_stock_margin_holding_details') }}
)

select
    pit.snapshot_date,
    h.account_id,
    
    -- 現物残高情報
    coalesce(sat_cash.quantity, 0) as cash_quantity,
    coalesce(sat_cash.average_cost, 0) as cash_avg_cost,
    (coalesce(sat_cash.quantity, 0) * coalesce(sat_cash.average_cost, 0)) as cash_market_value,
    
    -- 信用残高情報
    coalesce(sat_margin.side, 'N/A') as margin_side,
    coalesce(sat_margin.quantity, 0) as margin_quantity,
    coalesce(sat_margin.current_price, 0) as margin_current_price,
    coalesce(sat_margin.evaluation_profit_loss, 0) as margin_unrealized_pnl,
    
    -- 合計純資産 (現物評価額 + 信用評価損益)
    ((coalesce(sat_cash.quantity, 0) * coalesce(sat_cash.average_cost, 0)) + 
     coalesce(sat_margin.evaluation_profit_loss, 0)) as total_net_asset_value

from pit
inner join hub_acc h 
    on pit.account_hk = h.account_hk
-- PITテーブルに記録された「その時点で有効なLoad_Date」を使って等価結合
left join sat_cash 
    on pit.account_hk = sat_cash.account_hk 
    and pit.cash_holding_load_date = sat_cash.load_date
left join sat_margin 
    on pit.account_hk = sat_margin.account_hk 
    and pit.margin_holding_load_date = sat_margin.load_date