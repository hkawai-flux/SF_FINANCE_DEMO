INSERT INTO SBI_RAW_VAULT.HUB_THEME (
    THEME_HK,
    THEME_ID,
    LOAD_DATE,
    RECORD_SOURCE
)
SELECT 
    THEME_HK,
    THEME_ID,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE
FROM sbi_staging.stg_st_sec_test src
WHERE 
    THEME_ID IS NOT NULL -- テーマIDが存在するレコードのみ対象
    -- 既にHUBに存在するキーは除外
    AND NOT EXISTS (
        SELECT 1 FROM SBI_RAW_VAULT.HUB_THEME tgt
        WHERE src.THEME_HK = tgt.THEME_HK
    )
-- 重複排除：同一のテーマハッシュキーがある場合、最初の1件を採用
QUALIFY ROW_NUMBER() OVER (PARTITION BY THEME_HK ORDER BY LOAD_DATE ASC) = 1;