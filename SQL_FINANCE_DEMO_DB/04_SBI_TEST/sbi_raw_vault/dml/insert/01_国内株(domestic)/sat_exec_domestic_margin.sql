INSERT INTO SBI_RAW_VAULT.SAT_EXEC_DOMESTIC_MARGIN (
    EXECUTION_ACCOUNT_LK,
    EXEC_MARGIN_HASHDIFF,
    LOAD_DATE,
    RECORD_SOURCE,
    REPAYMENT_LIMIT
)
SELECT 
    -- リンクキー（約定-口座-銘柄）の生成：他の約定サテライトと統一
    SHA2_BINARY(concat_ws('|', 
        coalesce(cast(EXECUTION_HK as string), ''), 
        coalesce(cast(ACCOUNT_HK as string), ''), 
        coalesce(cast(BRAND_HK as string), '')
    ), 256) as EXECUTION_ACCOUNT_LK,
    
    -- 信用属性の変更検知用ハッシュ
    SHA2_BINARY(coalesce(cast(REPAYMENT_LIMIT as string), ''), 256) as EXEC_MARGIN_HASHDIFF,
    
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE,
    REPAYMENT_LIMIT
FROM sbi_staging.stg_st_sec_test src
WHERE 
    -- 信用取引のレコード（新規・決済・現引現渡）のみを対象とする
    SETTLE_CD IN ('6', '7', 'G')
-- 最新の約定データ（または一意な約定キー）ごとに1件を採用
QUALIFY ROW_NUMBER() OVER (PARTITION BY EXECUTION_HK ORDER BY LOAD_DATE DESC) = 1;