INSERT INTO SBI_RAW_VAULT.LINK_EXECUTION_ACCOUNT (
    EXECUTION_ACCOUNT_LK,
    EXECUTION_HK,
    ACCOUNT_HK,
    BRAND_HK,
    LOAD_DATE,
    RECORD_SOURCE
)
SELECT 
    -- リンクハッシュキーの動的生成
    SHA2_BINARY(concat_ws('|', 
        coalesce(cast(EXECUTION_HK as string), ''), 
        coalesce(cast(ACCOUNT_HK as string), ''), 
        coalesce(cast(BRAND_HK as string), '')
    ), 256) as EXECUTION_ACCOUNT_LK,
    EXECUTION_HK,
    ACCOUNT_HK,
    BRAND_HK,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE
FROM sbi_staging.stg_st_sec_test src
WHERE 
    EXECUTION_HK IS NOT NULL 
    AND ACCOUNT_HK IS NOT NULL 
    AND BRAND_HK IS NOT NULL
    -- すでにLinkに存在する組み合わせは除外
    AND NOT EXISTS (
        SELECT 1 FROM SBI_RAW_VAULT.LINK_EXECUTION_ACCOUNT tgt
        WHERE SHA2_BINARY(concat_ws('|', 
                coalesce(cast(src.EXECUTION_HK as string), ''), 
                coalesce(cast(src.ACCOUNT_HK as string), ''), 
                coalesce(cast(src.BRAND_HK as string), '')
              ), 256) = tgt.EXECUTION_ACCOUNT_LK
    )
-- キーの組み合わせごとに1件に絞り込む
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY EXECUTION_HK, ACCOUNT_HK, BRAND_HK 
    ORDER BY LOAD_DATE ASC
) = 1;