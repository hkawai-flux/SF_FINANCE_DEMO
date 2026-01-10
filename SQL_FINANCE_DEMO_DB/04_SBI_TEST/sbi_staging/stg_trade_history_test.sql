-- テスト用テーブルの作成
CREATE OR REPLACE TABLE sbi_staging.stg_trade_history_test (
    order_hk BINARY(64) NOT NULL COMMENT '注文ハッシュキー（店番-口座-注文番号）',
    account_hk BINARY(64) NOT NULL COMMENT '口座ハッシュキー（店番-口座）',
    brand_hk BINARY(64) NOT NULL COMMENT '銘柄ハッシュキー（銘柄コード）',
    order_hashdiff BINARY(64) NOT NULL COMMENT '注文属性ハッシュ差分（差分比較用）',
    base_month VARCHAR(6) COMMENT '基準月 (YYYYMM)',
    base_date DATE COMMENT '基準日 (YYYY-MM-DD)',
    order_no INT COMMENT '注文番号',
    buten_kouza VARCHAR COMMENT '店番-口座番号',
    trade_date VARCHAR COMMENT '取引日 (YYYY-MM-DD)',
    brand_cd VARCHAR(5) COMMENT '銘柄コード',
    br_brand_ind VARCHAR COMMENT '銘柄区分',
    formal_brand_name VARCHAR COMMENT '正式銘柄名',
    search_product_l_cd VARCHAR COMMENT '検索用商品大分類コード',
    search_trade_m_cd VARCHAR COMMENT '検索用取引中分類コード',
    trade_short_name2 VARCHAR COMMENT '取引略称2',
    hitokutei_kbn VARCHAR COMMENT '非特定区分',
    cancel_flg INT COMMENT '取消フラグ (0:有効, 1:取消)',
    price INT COMMENT '価格',
    quantity INT COMMENT '数量',
    fee INT COMMENT '手数料',
    target_date VARCHAR COMMENT '対象日',
    load_date TIMESTAMP_NTZ COMMENT 'ロード日時',
    record_source VARCHAR COMMENT 'レコードソース（データ元）',
    other_val VARIANT COMMENT 'その他属性（JSON形式）',
    
    PRIMARY KEY (order_hk)
) COMMENT = '取引履歴（テスト用ステージングテーブル）';

-- テスト用データの挿入
INSERT INTO sbi_staging.stg_trade_history_test
SELECT 
    -- 元のロジックに基づくハッシュ生成
    SHA2_BINARY(concat_ws('|', coalesce(SPLIT_PART(t.buten_kouza, '-', 1), ''), coalesce(SPLIT_PART(t.buten_kouza, '-', 2), ''), coalesce(cast(t.order_no as string), '')), 256) as order_hk,
    SHA2_BINARY(concat_ws('|', coalesce(SPLIT_PART(t.buten_kouza, '-', 1), ''), coalesce(SPLIT_PART(t.buten_kouza, '-', 2), '')), 256) as account_hk,
    SHA2_BINARY(coalesce(LPAD(t.brand_cd, 5, '0'), ''), 256) as brand_hk,
    SHA2_BINARY(concat_ws('|',
        coalesce(t.search_product_l_cd, ''),
        coalesce(t.search_trade_m_cd, ''),
        coalesce(cast(t.cancel_flg as string), ''),
        coalesce(cast(t.price as string), ''),
        coalesce(cast(t.quantity as string), ''),
        coalesce(cast(t.fee as string), '')), 256) as order_hashdiff,
    -- 基本データ項目
    t.base_month,
    TO_DATE(t.trade_date) as base_date,
    t.order_no,
    t.buten_kouza,
    t.trade_date,
    LPAD(t.brand_cd, 5, '0') as brand_cd,
    t.br_brand_ind,
    t.formal_brand_name,
    t.search_product_l_cd,
    t.search_trade_m_cd,
    t.trade_short_name2,
    t.hitokutei_kbn,
    t.cancel_flg,
    t.price,
    t.quantity,
    t.fee,
    t.target_date,
    TO_TIMESTAMP_NTZ(t.trade_date) as load_date,
    'GENERATED_TEST_DATA' as record_source,
    OBJECT_CONSTRUCT('BR_BRAND_IND', t.br_brand_ind, 'GENERATED_AT', CURRENT_TIMESTAMP()) as other_val
FROM (
    SELECT 
        '202510' as base_month,
        SEQ8() as order_no,
        -- ランダムな店番(3桁)-口座(7桁)の生成
        LPAD(UNIFORM(100, 999, RANDOM()), 3, '0') || '-' || LPAD(UNIFORM(1, 9999999, RANDOM()), 7, '0') as buten_kouza,
        -- 2025年10月の範囲でランダムな日付
        DATEADD(day, UNIFORM(0, 30, RANDOM()), '2025-10-01')::VARCHAR as trade_date,
        -- ランダムな4桁銘柄コード
        UNIFORM(1000, 9999, RANDOM())::VARCHAR as brand_cd,
        '1' as br_brand_ind,
        'テスト銘柄_' || brand_cd as formal_brand_name,
        '01' as search_product_l_cd,
        '10' as search_trade_m_cd,
        '現物買' as trade_short_name2,
        '0' as hitokutei_kbn,
        UNIFORM(0, 1, RANDOM()) as cancel_flg,
        UNIFORM(100, 10000, RANDOM()) as price,
        UNIFORM(1, 1000, RANDOM()) * 100 as quantity,
        UNIFORM(0, 500, RANDOM()) as fee,
        trade_date as target_date
    FROM TABLE(GENERATOR(ROWCOUNT => 100000)) -- 10万件生成
) t;

-- 確認用のSELECT文
SELECT * FROM sbi_staging.stg_trade_history_test LIMIT 10;