INSERT INTO SBI_RAW_VAULT.SAT_BRAND_DETAIL
SELECT 
    -- ハブと同一のロジックでHKを生成
    SHA2_BINARY(coalesce(LPAD(brand_cd, 5, '0'), ''), 256) as BRAND_HK,
    -- 全属性を連結して変更検知用のHashdiffを生成
    SHA2_BINARY(concat_ws('|',
        coalesce(brand_name, ''),
        coalesce(m_stock_listing_market, ''),
        coalesce(kind, ''),
        coalesce(sector33, ''),
        coalesce(stock_etf_kbn, ''),
        coalesce(old_mothers_kbn, '')
    ), 256) as BRAND_HASHDIFF,
    CURRENT_TIMESTAMP() as LOAD_DATE,
    'KPI_MASTER_TEST' as RECORD_SOURCE,
    brand_name,
    m_stock_listing_market,
    kind,
    sector33,
    stock_etf_kbn,
    old_mothers_kbn
FROM sbi_staging.v_kpi_kabu_master_test
QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_HK ORDER BY LOAD_DATE DESC) = 1;