INSERT INTO SBI_RAW_VAULT.SAT_EXEC_DOMESTIC
SELECT 
    SHA2_BINARY(concat_ws('|', 
        coalesce(cast(EXECUTION_HK as string), ''), 
        coalesce(cast(ACCOUNT_HK as string), ''), 
        coalesce(cast(BRAND_HK as string), '')
    ), 256) as EXECUTION_ACCOUNT_LK,
    -- 国内株固有のHashdiff（テーマHKが変更されたら新レコードを作成）
    SHA2_BINARY(coalesce(cast(THEME_HK as string), ''), 256) as EXECUTION_HASHDIFF,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE,
    THEME_HK
FROM sbi_staging.stg_st_sec_test src
WHERE THEME_HK IS NOT NULL -- 国内株特有のデータがある場合のみ
QUALIFY ROW_NUMBER() OVER (PARTITION BY EXECUTION_HK ORDER BY LOAD_DATE DESC) = 1;