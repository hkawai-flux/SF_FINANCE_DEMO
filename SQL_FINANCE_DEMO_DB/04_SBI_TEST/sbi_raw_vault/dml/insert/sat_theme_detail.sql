INSERT INTO SBI_RAW_VAULT.SAT_THEME_DETAIL (
    THEME_HK,
    THEME_HASHDIFF,
    THEME_ID,
    THEME_REGIST_MONTH,
    THEME_NAME,
    LOAD_DATE,
    RECORD_SOURCE
)
SELECT 
    theme_hk,
    theme_hashdiff,
    theme_id,
    theme_regist_month,
    theme_name,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    record_source
FROM SBI_STAGING.stg_theme_master src
QUALIFY ROW_NUMBER() OVER (PARTITION BY theme_hk ORDER BY load_date DESC) = 1;




TRUNCATE TABLE SBI_RAW_VAULT.SAT_THEME_DETAIL;

INSERT INTO SBI_RAW_VAULT.SAT_THEME_DETAIL
SELECT 
    theme_hk,
    theme_hashdiff,
    theme_id,
    theme_regist_month,
    theme_name,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    record_source
FROM SBI_STAGING.stg_theme_master;