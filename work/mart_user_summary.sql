-- models/core/mart_user_summary.sql

{{ config(
    materialized='table'
) }}

with executions as (
    -- 先ほど作成した STAGING 層のモデルを参照します
    select * from {{ ref('stg_executions') }}
),

final as (
    select
        account_id,
        stock_symbol,
        trade_type,
        trade_quantity,
        avg(trade_price) as avg_cost_price,
        count(execution_id) as total_executions,
        sum(trade_quantity) as total_amount,
        min(executed_at) as first_execution_at,
        max(executed_at) as last_execution_at
    from executions
    group by 1,2,3,4
)

select * from final