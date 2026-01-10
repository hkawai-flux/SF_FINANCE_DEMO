-- 株銘柄マスタ（KPI向け）テスト用テーブルの作成
CREATE OR REPLACE TABLE sbi_staging.v_kpi_kabu_master_test (
    brand_cd VARCHAR(10) NOT NULL COMMENT '銘柄コード',
    brand_name VARCHAR COMMENT '銘柄名',
    m_stock_listing_market VARCHAR COMMENT '銘柄上場市場',
    kind VARCHAR COMMENT '銘柄東証市場区分',
    sector33 VARCHAR COMMENT '銘柄業種分類',
    stock_etf_kbn VARCHAR COMMENT '個別株／etf区分',
    old_mothers_kbn VARCHAR COMMENT '旧マザーズ区分',
    
    PRIMARY KEY (brand_cd)
) COMMENT = '株銘柄マスタ（KPI向け・テスト用物理テーブル）';

-- テストデータの挿入
INSERT INTO sbi_staging.v_kpi_kabu_master_test
SELECT 
    LPAD(base.id, 5, '0') as brand_cd, -- 銘柄コード(Lpad)
    'テスト銘柄_' || brand_cd as brand_name,
    CASE UNIFORM(1, 3, RANDOM()) 
        WHEN 1 THEN 'プライム' 
        WHEN 2 THEN 'スタンダード' 
        ELSE 'グロース' 
    END as m_stock_listing_market,
    '東証一部相当' as kind,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN '電気機器'
        WHEN 2 THEN '情報・通信業'
        WHEN 3 THEN '銀行業'
        WHEN 4 THEN '小売業'
        ELSE 'サービス業'
    END as sector33,
    decode(UNIFORM(1, 2, RANDOM()), 1, '個別株', 'ETF') as stock_etf_kbn,
    -- 元SQLのCASE文ロジックを再現（約10%の確率で旧マザーズとする）
    CASE 
        WHEN UNIFORM(1, 10, RANDOM()) = 1 THEN '旧マザーズ'
        ELSE NULL 
    END as old_mothers_kbn
FROM (
    SELECT SEQ8() + 1000 as id
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
) base;