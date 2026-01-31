INSERT INTO SBI_RAW_VAULT.LINK_ACCOUNT_POSITION (
    ACCOUNT_POSITION_LK,
    ACCOUNT_HK,
    POSITION_HK,
    BRAND_HK,
    LOAD_DATE,
    RECORD_SOURCE
)
SELECT 
    -- リンクハッシュキーの動的生成
    SHA2_BINARY(concat_ws('|', 
        coalesce(cast(ACCOUNT_HK as string), ''), 
        coalesce(cast(POSITION_HK as string), ''),
        coalesce(cast(BRAND_HK as string), '')
    ), 256) as ACCOUNT_POSITION_LK,
    ACCOUNT_HK,
    POSITION_HK,
    BRAND_HK,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE
FROM (
    -- 1. 国内株式現物預り明細から抽出
    SELECT ACCOUNT_HK, POSITION_HK, BRAND_HK, RECORD_SOURCE FROM sbi_staging.stg_bl_int_stock_test
    UNION ALL
    -- 2. 国内株式信用建玉明細から抽出
    SELECT ACCOUNT_HK, POSITION_HK, BRAND_HK, RECORD_SOURCE FROM sbi_staging.stg_bl_trust_stock_test
) src
WHERE 
    ACCOUNT_HK IS NOT NULL 
    AND POSITION_HK IS NOT NULL
    AND BRAND_HK IS NOT NULL
    -- すでにLinkに存在する組み合わせは除外
    AND NOT EXISTS (
        SELECT 1 FROM SBI_RAW_VAULT.LINK_ACCOUNT_POSITION tgt
        WHERE SHA2_BINARY(concat_ws('|', 
                coalesce(cast(src.ACCOUNT_HK as string), ''), 
                coalesce(cast(src.POSITION_HK as string), ''),
                coalesce(cast(BRAND_HK as string), '')
              ), 256) = tgt.ACCOUNT_POSITION_LK
    )
-- 重複排除：同一の組み合わせがある場合、最初の1件を採用
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ACCOUNT_HK, POSITION_HK , BRAND_HK
    ORDER BY LOAD_DATE ASC
) = 1;