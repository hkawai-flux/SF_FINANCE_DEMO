-- 信用約定明細テスト用テーブルの作成
CREATE OR REPLACE TABLE sbi_staging.stg_tmp_tran_trust_stock_test (
    order_hk BINARY(64) NOT NULL COMMENT '注文ハッシュキー（注文番号-枝番-店番口座）',
    account_hk BINARY(64) NOT NULL COMMENT '口座ハッシュキー（店番-口座）',
    brand_hk BINARY(64) NOT NULL COMMENT '銘柄ハッシュキー（銘柄コード+枝番）',
    order_hashdiff BINARY(64) NOT NULL COMMENT '注文属性ハッシュ差分（価格・数量・手数料等）',
    base_month VARCHAR(6) COMMENT '基準月 (YYYYMM)',
    base_date DATE COMMENT '基準日 (YYYY-MM-DD)',
    order_no INT COMMENT '注文番号',
    sequence_no INT COMMENT '取引枝番',
    trade_date DATE COMMENT '取引日',
    buten_kouza VARCHAR COMMENT '店番-口座番号',
    brand_cd VARCHAR COMMENT '銘柄コード（連結後）',
    search_trade_m_cd VARCHAR COMMENT '検索用取引中分類コード',
    search_trade_s_cd VARCHAR COMMENT '検索用取引小分類コード',
    cancel VARCHAR COMMENT '取消区分',
    price INT COMMENT '単価',
    quantity INT COMMENT '数量',
    fee INT COMMENT '手数料',
    loan_rate INT COMMENT '貸借料率',
    general_margin_id VARCHAR COMMENT '一般信用識別ID',
    sitei_atukai_kbn VARCHAR COMMENT '指定扱い区分',
    kinri_jouken_kbn VARCHAR COMMENT '金利条件区分',
    back_loan_rate_only VARCHAR COMMENT '逆日歩のみフラグ',
    lending_fee INT COMMENT '品貸料',
    target_date VARCHAR COMMENT '対象日',
    load_date TIMESTAMP_NTZ COMMENT 'ロード日時',
    record_source VARCHAR COMMENT 'レコードソース',
    other_val VARIANT COMMENT 'その他属性（グループID、クラスコード等）',
    
    PRIMARY KEY (order_hk)
) COMMENT = '信用約定明細（テスト用ステージングテーブル）';

---テストデータのロード
INSERT INTO sbi_staging.stg_tmp_tran_trust_stock_test
SELECT 
    -- ハッシュ生成ロジック
    SHA2_BINARY(concat_ws('|', coalesce(cast(order_no as string), ''), coalesce(cast(sequence_no as string), ''), coalesce(cast(buten_kouza as string), '')), 256) as order_hk,
    SHA2_BINARY(concat_ws('|', coalesce(SPLIT_PART(buten_kouza, '-', 1), ''), coalesce(SPLIT_PART(buten_kouza, '-', 2), '')), 256) as account_hk,
    SHA2_BINARY(brand_cd, 256) as brand_hk,
    SHA2_BINARY(
        concat_ws('|',
        coalesce(search_trade_m_cd, ''),
        coalesce(search_trade_s_cd, ''),
        coalesce(cancel, ''),
        coalesce(cast(price as string), ''),
        coalesce(cast(quantity as string), ''),
        coalesce(cast(fee as string), '')), 256) as order_hashdiff,
    
    -- 基本データ項目
    base_month,
    base_date,
    order_no,
    sequence_no,
    TO_DATE(trade_date, 'YYYYMMDD') as trade_date,
    buten_kouza,
    brand_cd,
    search_trade_m_cd,
    search_trade_s_cd,
    cancel,
    price,
    quantity,
    fee,
    loan_rate,
    general_margin_id,
    sitei_atukai_kbn,
    kinri_jouken_kbn,
    back_loan_rate_only,
    lending_fee,
    target_date,
    TO_TIMESTAMP_NTZ(base_date) as load_date,
    'TEST_GEN_TRAN_TRUST' as record_source,
    OBJECT_CONSTRUCT(
        'GROUP_ID', 'GRP-' || LPAD(UNIFORM(1, 99, RANDOM()), 2, '0'),
        'CLASS_CODE', 'CLS-' || LPAD(UNIFORM(1, 9, RANDOM()), 1, '0')
    ) as other_val
FROM (
    SELECT 
        '202510' as base_month,
        TO_DATE('2025-10-15') as base_date,
        SEQ8() as order_no,
        UNIFORM(1, 5, RANDOM()) as sequence_no,
        '20251015' as trade_date,
        LPAD(UNIFORM(100, 999, RANDOM()), 3, '0') || '-' || LPAD(UNIFORM(1, 9999999, RANDOM()), 7, '0') as buten_kouza,
        LPAD(UNIFORM(1000, 9999, RANDOM()), 5, '0') || '0' as brand_cd,
        '20' as search_trade_m_cd, -- 信用取引想定
        '21' as search_trade_s_cd,
        '0' as cancel,
        UNIFORM(1000, 5000, RANDOM()) as price,
        UNIFORM(1, 100, RANDOM()) * 100 as quantity,
        UNIFORM(100, 1000, RANDOM()) as fee,
        UNIFORM(0, 3, RANDOM()) as loan_rate,
        'G-ID-' || UNIFORM(1, 5, RANDOM()) as general_margin_id,
        '1' as sitei_atukai_kbn,
        'A' as kinri_jouken_kbn,
        '0' as back_loan_rate_only,
        UNIFORM(0, 100, RANDOM()) as lending_fee,
        '2025-10-15' as target_date
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
) t;
