-- 国内株式信用建玉明細テスト用テーブルの作成
CREATE OR REPLACE TABLE sbi_staging.stg_bl_trust_stock_test (
    position_hk BINARY(64) NOT NULL COMMENT '残高ハッシュキー（明細番号-店番口座）',
    account_hk BINARY(64) NOT NULL COMMENT '口座ハッシュキー（店番-口座）',
    brand_hk BINARY(64) NOT NULL COMMENT '銘柄ハッシュキー（銘柄コード+枝番）',
    position_hashdiff BINARY(64) NOT NULL COMMENT '残高属性ハッシュ差分（評価単価・損益等）',
    base_month VARCHAR(6) COMMENT '基準月 (YYYYMM)',
    base_date DATE COMMENT '基準日 (YYYY-MM-DD)',
    position_no INT COMMENT '明細番号',
    buten_kouza VARCHAR COMMENT '店番-口座番号',
    brand_cd VARCHAR COMMENT '銘柄コード（連結後）',
    price_mx_revise_value INT COMMENT '修正評価単価',
    exec_base_balance_t1 INT COMMENT '約定ベース残高',
    appraise_aquire_price INT COMMENT '評価取得価額',
    appraise_market_price INT COMMENT '評価時価',
    appraise_profit_loss_price INT COMMENT '評価損益額',
    target_date VARCHAR COMMENT '対象日',
    load_date TIMESTAMP_NTZ COMMENT 'ロード日時',
    record_source VARCHAR COMMENT 'レコードソース',
    other_val VARIANT COMMENT 'その他属性（決済市場等）',
    
    PRIMARY KEY (position_hk)
) COMMENT = '国内株式信用建玉明細（テスト用ステージングテーブル）';

INSERT INTO sbi_staging.stg_bl_trust_stock_test
SELECT 
    -- ハッシュ生成ロジック (SHA2_BINARY 256の結果をBINARY(64)に格納)
    SHA2_BINARY(concat_ws('|', coalesce(cast(position_no as string), ''), coalesce(cast(buten_kouza as string), '')), 256) as position_hk,
    SHA2_BINARY(concat_ws('|', coalesce(SPLIT_PART(buten_kouza, '-', 1), ''), coalesce(SPLIT_PART(buten_kouza, '-', 2), '')), 256) as account_hk,
    SHA2_BINARY(brand_cd, 256) as brand_hk,
    SHA2_BINARY(
        concat_ws('|',
        coalesce(cast(price_mx_revise_value as string), ''),
        coalesce(cast(appraise_aquire_price as string), ''),
        coalesce(cast(appraise_market_price as string), ''),
        coalesce(cast(appraise_profit_loss_price as string), '')
        ), 256) as position_hashdiff,
    
    -- 基本データ
    base_month,
    base_date,
    position_no,
    buten_kouza,
    brand_cd,
    price_mx_revise_value,
    exec_base_balance_t1,
    appraise_aquire_price,
    appraise_market_price,
    appraise_profit_loss_price,
    target_date,
    TO_TIMESTAMP_NTZ(base_date) as load_date,
    'TEST_GEN_BL_TRUST_STOCK' as record_source,
    OBJECT_CONSTRUCT(
        'PAYMENT_MARKET', decode(UNIFORM(1, 3, RANDOM()), 1, '東証', 2, '名証', 'PTS')
    ) as other_val
FROM (
    SELECT 
        '202510' as base_month,
        TO_DATE('2025-10-31') as base_date,
        SEQ8() as position_no,
        LPAD(UNIFORM(100, 999, RANDOM()), 3, '0') || '-' || LPAD(UNIFORM(1, 9999999, RANDOM()), 7, '0') as buten_kouza,
        -- br_comp_code(5桁) + br_n_o_id(1桁)
        LPAD(UNIFORM(1000, 9999, RANDOM()), 5, '0') || '0' as brand_cd,
        UNIFORM(500, 20000, RANDOM()) as price_mx_revise_value, -- 修正評価単価
        UNIFORM(1, 100, RANDOM()) * 100 as exec_base_balance_t1, -- 建玉数量
        (price_mx_revise_value * 1.05)::INT as acquire_unit_price, -- 取得単価（信用なので少し高く設定）
        (acquire_unit_price * exec_base_balance_t1) as appraise_aquire_price, -- 取得価額
        (price_mx_revise_value * exec_base_balance_t1) as appraise_market_price, -- 時価
        (appraise_market_price - appraise_aquire_price) as appraise_profit_loss_price, -- 評価損益
        '2025-10-31' as target_date
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
) t;