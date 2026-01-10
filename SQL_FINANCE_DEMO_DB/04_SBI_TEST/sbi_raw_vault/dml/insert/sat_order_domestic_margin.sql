INSERT INTO SBI_RAW_VAULT.SAT_ORDER_DOMESTIC_MARGIN
SELECT 
    SHA2_BINARY(concat_ws('|', 
        coalesce(cast(ORDER_HK as string), ''), 
        coalesce(cast(ACCOUNT_HK as string), ''), 
        coalesce(cast(BRAND_HK as string), '')
    ), 256) as ORDER_ACCOUNT_LK,
    -- 信用属性のみを連結してHashdiffを生成（差分管理用）
    SHA2_BINARY(concat_ws('|', 
        coalesce(cast(LOAN_RATE as string), ''), 
        coalesce(GENERAL_MARGIN_ID, ''), 
        coalesce(SITEI_ATUKAI_KBN, ''), 
        coalesce(KINRI_JOUKEN_KBN, ''), 
        coalesce(cast(LENDING_FEE as string), '')
    ), 256) as ORDER_HASHDIFF,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE,
    LOAN_RATE,
    GENERAL_MARGIN_ID,
    SITEI_ATUKAI_KBN,
    KINRI_JOUKEN_KBN,
    BACK_LOAN_RATE_ONLY,
    LENDING_FEE,
    other_val:GROUP_ID::string,
    other_val:CLASS_CODE::string
FROM sbi_staging.stg_tmp_tran_trust_stock_test src
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_HK ORDER BY LOAD_DATE DESC) = 1;