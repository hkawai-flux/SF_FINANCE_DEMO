-- models/staging/stg_executions.sql

with source as (
    select * from {{ source('finance_raw', 'executions') }}
),

renamed as (
    select
        execution_id,
        account_id,
        symbol as stock_symbol,
        side as trade_side,
        'BUY' as trade_type,
        quantity as trade_quantity,
        price as trade_price,
        execution_at as executed_at,
        commission
    from source
)

select * from renamed