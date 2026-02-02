{{
  config(
    materialized='table',
    schema='SEMANTIC'
  )
}}

/*
  分析用モデルのベーステンプレート
  - 既存の Staging モデルを ref() で参照
  - CTE で加工し、最終 SELECT で分析用指標を出力
  - 新しい分析モデルは本ファイルをコピーし、参照元と集計ロジックを変更して利用
*/

with staging_source as (
    -- 利用する Staging モデルを ref で指定（例: 国内現物残高）
    select
        base_date,
        account_hk,
        brand_hk,
        brand_cd,
        quantity,
        average_cost,
        load_date,
        record_source
    from {{ ref('stg_stock_cash_holdings') }}
    -- 必要に応じて他の Staging を UNION / JOIN
    -- from {{ ref('stg_stock_margin_holdings') }}
),

aggregated as (
    select
        base_date,
        account_hk,
        count(distinct brand_cd) as brand_count,
        sum(quantity) as total_quantity,
        sum(quantity * average_cost) as total_cost_basis
    from staging_source
    group by base_date, account_hk
)

select
    base_date as "基準日",
    account_hk as "口座ハッシュキー",
    brand_count as "銘柄数",
    total_quantity as "合計数量",
    total_cost_basis as "取得原価合計"
from aggregated
order by base_date desc, account_hk
