with base_metrics as (
    select
        p.as_of_date,
        -- Satelliteから属性を直接取得 (Dimension的役割)
        sd.segment,
        sd.open_date,
        p.account_hk,
        
        -- 1. 残高保有判定 (指標8, 9, 10に対応)
        -- sat_stock_cash_holdings を PITのポインタで結合
        case when sh.quantity > 0 then 1 else 0 end as has_balance,
        
        -- 2. 稼働判定 (指標1, 2, 3, 6, 7に対応)
        -- 注文(Order)や約定(Execution)があったかを判定
        -- ここでは例として sat_stock_cash_orders の有無を判定に使用
        case when so.order_hk is not null then 1 else 0 end as is_active,
        
        -- 3. 新規開設判定 (指標11に対応)
        case when sd.open_date = p.as_of_date then 1 else 0 end as is_new_open
        
    from {{ ref('pit_account') }} p
    -- 口座属性 Satellite
    inner join {{ ref('sat_customer_details') }} sd
        on p.account_hk = sd.account_hk 
        and p.customer_load_date = sd.load_date
    -- 保有残高 Satellite (Left Join: 残高がない口座も含むため)
    left join {{ ref('sat_stock_cash_holdings') }} sh
        on p.account_hk = sh.account_hk 
        and p.cash_holding_load_date = sh.load_date
    -- 稼働判定用：注文 Satellite (必要に応じて追加)
    left join {{ ref('sat_stock_cash_orders') }} so
        on p.account_hk = so.account_hk
        -- 取引の「稼働」は「その日」に起きたことなので、as_of_dateと直接比較
        and cast(so.load_date as date) = p.as_of_date
),
aggr_metrics as (
    select
        as_of_date,
        segment,
        -- 指標1: 稼働口座数
        count(distinct case when is_active = 1 then account_hk end) as "稼働口座数",
        -- 指標9: 現物残高保有口座数
        count(distinct case when has_balance = 1 then account_hk end) as "現物残高保有口座数",
        -- 指標11: 新規開設数
        count(distinct case when is_new_open = 1 then account_hk end) as "新規開設数"
    from base_metrics
    group by 1, 2
)