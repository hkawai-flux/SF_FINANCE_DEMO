{{ config(materialized='view') }}

-- 1. 口座ごとの最新属性と稼働フラグの集約
WITH active_snapshot AS (
    SELECT
        p.as_of_date,
        sd.segment,
        sd.load_date,
        p.account_hk,
        
        -- 指標9: 現物残高保有判定
        CASE WHEN sh.quantity > 0 THEN 1 ELSE 0 END AS has_stock_balance,
        
        -- 指標1: 稼働判定 (例として残高変動やマスタ更新日を基準にするか、
        -- 実際に取引Satがあればそちらに差し替えてください)
        CASE WHEN p.cash_holding_load_date = p.as_of_date THEN 1 ELSE 0 END AS is_active_today,

        -- 指標3: 継続稼働判定用の過去フラグ（Window関数）
        MAX(CASE WHEN p.cash_holding_load_date IS NOT NULL THEN 1 ELSE 0 END) OVER (
            PARTITION BY p.account_hk 
            ORDER BY p.as_of_date 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS was_active_last_month

    FROM {{ ref('pit_account') }} p
    INNER JOIN {{ ref('sat_customer_details') }} sd
        ON p.account_hk = sd.account_hk 
        AND p.customer_load_date = sd.load_date
    LEFT JOIN {{ ref('sat_stock_cash_holdings') }} sh
        ON p.account_hk = sh.account_hk 
        AND p.cash_holding_load_date = sh.load_date
)

-- 2. 画像(image_914aec.png)の要件に合わせた集計
SELECT
    as_of_date AS "基準日",
    segment AS "セグメント",
    COUNT(DISTINCT CASE WHEN is_active_today = 1 THEN account_hk END) AS "稼働口座数", -- 指標1
    COUNT(DISTINCT CASE WHEN is_active_today = 1 AND was_active_last_month = 0 THEN account_hk END) AS "新規稼働数", -- 指標2
    COUNT(DISTINCT CASE WHEN is_active_today = 1 AND was_active_last_month = 1 THEN account_hk END) AS "継続稼働数", -- 指標3
    COUNT(DISTINCT CASE WHEN has_stock_balance = 1 THEN account_hk END) AS "現物残高保有口座数", -- 指標9
    COUNT(DISTINCT CASE WHEN load_date = as_of_date THEN account_hk END) AS "新規開設数" -- 指標11
FROM active_snapshot
GROUP BY 1, 2