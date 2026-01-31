INSERT INTO SBI_RAW_VAULT.SAT_EXECUTION_CORE
SELECT 
    -- リンクキーの生成（Linkテーブルと同一ロジック）
    SHA2_BINARY(concat_ws('|', 
        coalesce(cast(EXECUTION_HK as string), ''), 
        coalesce(cast(ACCOUNT_HK as string), ''), 
        coalesce(cast(BRAND_HK as string), '')
    ), 256) as EXECUTION_ACCOUNT_LK,
    EXECUTION_HASHDIFF,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    RECORD_SOURCE,
    BASE_MONTH,
    BASE_DATE,
    TRADE_EXEC_DATE,
    CANCEL_FLG,
    ORDER_TYPE,
    BUY_SELL,
    MARKET_CD,
    SETTLE_CD,
    HITOKUTEI_TRADE_KBN,
    COMMISSION_FLG,
    ROUTE,
    EXEC_QUANTITY,
    EXEC_PRICE,
    EXEC_AMOUNT,
    KEIJOU_QUANTITY,
    KEIJOU_COMMISSION,
    KEIJOU_AMOUNT
FROM sbi_staging.stg_st_sec_test src
QUALIFY ROW_NUMBER() OVER (PARTITION BY EXECUTION_HK ORDER BY LOAD_DATE DESC) = 1;