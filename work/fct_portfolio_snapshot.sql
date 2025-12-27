-- models/semantic/fct_portfolio_snapshot.sql

{{ config(
    materialized='table'
) }}

with current_positions as (
    -- CORE層（またはStaging）から「現在の保有数量」を取得
    -- 本来はPOSITIONSテーブルを使いますが、今回は簡易的に集計
    select
        account_id,
        stock_symbol,
        sum(case when trade_type = 'BUY' then trade_quantity else -trade_quantity end) as current_quantity,
        avg(trade_price) as avg_cost_price
    from {{ ref('stg_executions') }}
    group by 1, 2
),

final as (
    select
        account_id,
        stock_symbol,
        current_quantity,
        avg_cost_price,
        -- ここで「最新時価」と掛け合わせる（今回はデモ用に最新価格を1.1倍と仮定）
        (current_quantity * avg_cost_price * 1.1) as estimated_market_value,
        ((avg_cost_price * 1.1) - avg_cost_price) * current_quantity as unrealized_profit_loss
    from current_positions
    where current_quantity > 0
)

select * from final