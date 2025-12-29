-- models/staging/stg_foreign_margin_holdings.sql
SELECT
    -- ハッシュキー生成 (SHA2_BINARY 256)
    sha2_binary(concat_ws('|', coalesce(ACCOUNT_ID, ''), coalesce(BRAND_CD, '')), 256) as ACCOUNT_BRAND_HK,
    sha2_binary(ACCOUNT_ID, 256) as ACCOUNT_HK,
    sha2_binary(BRAND_CD, 256) as BRAND_HK,
    
    -- 属性ハッシュ（数量や価格の変化を検知）
    sha2_binary(concat_ws('|', 
        coalesce(cast(QUANTITY as string), ''), 
        coalesce(cast(OPEN_PRICE_JPY as string), ''),
        coalesce(SIDE, '')
    ), 256) as MARGIN_HOLDING_HASHDIFF,

    cast(EXTRACT_DATE as date) as base_date, 
    ACCOUNT_ID,
    BRAND_CD,
    SIDE,
    QUANTITY,
    OPEN_PRICE_LOCAL,
    OPEN_PRICE_JPY,
    INTEREST_JPY,
    EXTRACT_DATE,
    current_timestamp() as LOAD_DATE,
    'FOREIGN_TRADING_SYSTEM' as RECORD_SOURCE
FROM {{ source('finance_raw', 'foreign_margin_holdings') }}