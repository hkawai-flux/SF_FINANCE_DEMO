{{ config(materialized='incremental', incremental_strategy='merge', unique_key='execution_hk') }}

with union_executions as (
    select execution_hk, execution_id, load_date, record_source from {{ ref('stg_stock_cash_executions') }}
    union
    select execution_hk, execution_id, load_date, record_source from {{ ref('stg_stock_margin_executions') }}
    union
    select execution_hk, execution_id, load_date, record_source from {{ ref('stg_foreign_stock_executions') }}
),
distinct_execs as (
    select
        execution_hk,
        execution_id,
        load_date,
        record_source,
        row_number() over (partition by execution_hk order by load_date asc) as rnum
    from union_executions
)
select
    execution_hk,
    execution_id,
    load_date,
    record_source
from distinct_execs
where rnum = 1
{% if is_incremental() %}
  and execution_hk not in (select execution_hk from {{ this }})
{% endif %}