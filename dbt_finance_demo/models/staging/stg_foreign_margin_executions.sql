{{ config(materialized='view') }}

SELECT
    -- ハッシュキー: 以前決めた通り BINARY(32) / SHA2_BINARY(..., 256)
    sha2_binary(concat_ws('|', coalesce(ACCOUNT_ID, ''), coalesce(BRAND_CD, '')), 256) as ACCOUNT_BRAND_HK,
    sha2_binary(ACCOUNT_ID, 256) as ACCOUNT_HK,
    sha2_binary(BRAND_CD, 256) as BRAND_HK,
    
    -- 属性ハッシュ（変更検知用）
    sha2_binary(concat_ws('|', 
        coalesce(cast(EXECUTION_QTY as string), ''), 
        coalesce(cast(EXECUTION_AMOUNT_JPY as string), '')
    ), 256) as EXECUTION_HASHDIFF,

    TO_DATE(EXECUTION_TIMESTAMP) as base_date,
    EXECUTION_ID,
    ACCOUNT_ID,
    BRAND_CD,
    SIDE,
    EXECUTION_QTY,
    EXECUTION_AMOUNT_JPY,
    EXECUTION_TIMESTAMP,
    current_timestamp() as LOAD_DATE,
    'FOREIGN_TRADING_SYSTEM' as RECORD_SOURCE
FROM {{ source('finance_raw', 'foreign_margin_executions') }}