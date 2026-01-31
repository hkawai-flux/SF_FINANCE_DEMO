INSERT INTO SBI_RAW_VAULT.HUB_BRAND (
    BRAND_HK,
    BRAND_CD,
    LOAD_DATE,
    RECORD_SOURCE
)
SELECT 
    BRAND_HK,
    BRAND_CD,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE
FROM (
    -- 1. 取引履歴（現物）
    SELECT BRAND_HK, BRAND_CD, RECORD_SOURCE FROM sbi_staging.stg_trade_history_test
    UNION ALL
    -- 2. 約定明細
    SELECT BRAND_HK, BRAND_CD, RECORD_SOURCE FROM sbi_staging.stg_st_sec_test
    UNION ALL
    -- 3. 株銘柄マスタ（ブランドHKを動的生成）
    SELECT 
        SHA2_BINARY(coalesce(LPAD(brand_cd, 5, '0'), ''), 256) as BRAND_HK, 
        brand_cd as BRAND_CD, 
        'KPI_MASTER' as RECORD_SOURCE 
    FROM sbi_staging.v_kpi_kabu_master_test
    UNION ALL
    -- 4. 国内株式現物預り明細
    SELECT BRAND_HK, BRAND_CD, RECORD_SOURCE FROM sbi_staging.stg_bl_int_stock_test
    UNION ALL
    -- 5. 国内株式信用建玉明細
    SELECT BRAND_HK, BRAND_CD, RECORD_SOURCE FROM sbi_staging.stg_bl_trust_stock_test
    UNION ALL
    -- 6. 信用約定明細
    SELECT BRAND_HK, BRAND_CD, RECORD_SOURCE FROM sbi_staging.stg_tmp_tran_trust_stock_test
    UNION ALL
    -- 7. 外国株式約定明細
    SELECT BRAND_HK, BRAND_CD, RECORD_SOURCE FROM sbi_staging.stg_foreign_stock_test
    UNION ALL
    -- 8. 外国株式銘柄マスタ
    SELECT BRAND_HK, BRAND_CD, RECORD_SOURCE FROM sbi_staging.stg_foreign_master_test
) src
-- 重複排除：同一銘柄が複数のソースにある場合、最初の1件を採用
QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_HK ORDER BY LOAD_DATE ASC) = 1
-- 既にHUBに登録済みの銘柄は除外
AND NOT EXISTS (
    SELECT 1 FROM SBI_RAW_VAULT.HUB_BRAND tgt
    WHERE src.BRAND_HK = tgt.BRAND_HK
);