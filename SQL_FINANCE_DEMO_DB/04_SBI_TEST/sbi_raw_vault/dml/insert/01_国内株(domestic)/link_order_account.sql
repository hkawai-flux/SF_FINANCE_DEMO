INSERT INTO SBI_RAW_VAULT.LINK_ORDER_ACOUNT (
    ORDER_ACCOUNT_LK,
    ORDER_HK,
    ACCOUNT_HK,
    BRAND_HK,
    LOAD_DATE,
    RECORD_SOURCE
)
SELECT 
    -- リンクハッシュキーの動적生成
    SHA2_BINARY(concat_ws('|', 
        coalesce(cast(ORDER_HK as string), ''), 
        coalesce(cast(ACCOUNT_HK as string), ''), 
        coalesce(cast(BRAND_HK as string), '')
    ), 256) as ORDER_ACCOUNT_LK,
    ORDER_HK,
    ACCOUNT_HK,
    BRAND_HK,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE
FROM (
    -- 現物取引履歴から抽出
    SELECT ORDER_HK, ACCOUNT_HK, BRAND_HK, RECORD_SOURCE FROM sbi_staging.stg_trade_history_test
    UNION ALL
    -- 信用約定明細から抽出
    SELECT ORDER_HK, ACCOUNT_HK, BRAND_HK, RECORD_SOURCE FROM sbi_staging.stg_tmp_tran_trust_stock_test
) src
WHERE 
    ORDER_HK IS NOT NULL 
    AND ACCOUNT_HK IS NOT NULL 
    AND BRAND_HK IS NOT NULL
    -- すでにLinkに存在する組み合わせは除外
    AND NOT EXISTS (
        SELECT 1 FROM SBI_RAW_VAULT.LINK_ORDER_ACOUNT tgt
        WHERE SHA2_BINARY(concat_ws('|', 
                coalesce(cast(src.ORDER_HK as string), ''), 
                coalesce(cast(src.ACCOUNT_HK as string), ''), 
                coalesce(cast(src.BRAND_HK as string), '')
              ), 256) = tgt.ORDER_ACCOUNT_LK
    )
-- 重複排除：同一の組み合わせがある場合、最初の1件を採用
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ORDER_HK, ACCOUNT_HK, BRAND_HK 
    ORDER BY LOAD_DATE ASC
) = 1;