INSERT INTO SBI_RAW_VAULT.SAT_POS_DOMESTIC_CASH
SELECT 
    SHA2_BINARY(concat_ws('|', coalesce(cast(ACCOUNT_HK as string), ''), coalesce(cast(POSITION_HK as string), '')), 256) as ACCOUNT_POSITION_LK,
    SHA2_BINARY(concat_ws('|', coalesce(other_val:JNISA_ACCOUNT_KBN::string, ''), coalesce(other_val:DEALER_NO::string, '')), 256) as POSITION_HASHDIFF,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE,
    other_val:JNISA_ACCOUNT_KBN::string,
    other_val:DEALER_NO::string
FROM sbi_staging.stg_bl_int_stock_test
QUALIFY ROW_NUMBER() OVER (PARTITION BY POSITION_HK ORDER BY LOAD_DATE DESC) = 1;