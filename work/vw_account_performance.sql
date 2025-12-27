-- models/semantic/vw_account_performance.sql

{{ config(
    materialized='view' 
) }}

with portfolio as (
    select * from {{ ref('fct_portfolio_snapshot') }}
),

account_summary as (
    select
        account_id,
        count(distinct stock_symbol) as holding_stocks_count,
        sum(estimated_market_value) as total_market_value,
        sum(unrealized_profit_loss) as total_unrealized_profit_loss,
        -- 評価損益率の計算
        case 
            when sum(current_quantity * avg_cost_price) > 0 
            then (sum(unrealized_profit_loss) / sum(current_quantity * avg_cost_price)) * 100
            else 0 
        end as profit_loss_percentage
    from portfolio
    group by 1
)

select * from account_summary