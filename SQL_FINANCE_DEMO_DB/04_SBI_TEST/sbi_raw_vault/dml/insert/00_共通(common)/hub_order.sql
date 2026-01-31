INSERT INTO SBI_RAW_VAULT.HUB_ORDER (
    ORDER_HK,
    BUTEN_KOUZA,
    ORDER_NO,
    LOAD_DATE,
    RECORD_SOURCE
)
SELECT 
    ORDER_HK,
    BUTEN_KOUZA,
    ORDER_NO,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE
FROM (
    -- 現物取引履歴（注文単位）から抽出
    SELECT ORDER_HK, BUTEN_KOUZA, ORDER_NO, RECORD_SOURCE FROM sbi_staging.stg_trade_history_test
    UNION ALL
    -- 信用約定明細（注文単位）から抽出
    SELECT ORDER_HK, BUTEN_KOUZA, ORDER_NO, RECORD_SOURCE FROM sbi_staging.stg_tmp_tran_trust_stock_test
) src
-- 重複排除：同一の注文ハッシュキーが複数ソースにある場合、最初の1件を採用
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_HK ORDER BY LOAD_DATE ASC) = 1
-- すでにHUBに存在する注文は除外
AND NOT EXISTS (
    SELECT 1 FROM SBI_RAW_VAULT.HUB_ORDER tgt
    WHERE src.ORDER_HK = tgt.ORDER_HK
);