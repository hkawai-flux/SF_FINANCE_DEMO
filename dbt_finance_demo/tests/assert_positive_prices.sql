-- 価格が0以下のレコードがあればエラーを出す（列数を一致させる）
select
    execution_id,
    price,
    record_source
from {{ ref('stg_stock_cash_executions') }}
where price <= 0

union all

select
    execution_id,
    price,
    record_source
from {{ ref('stg_stock_margin_executions') }}
where price <= 0