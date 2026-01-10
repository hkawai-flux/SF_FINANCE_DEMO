-- テーママスタ・ステージングの作成
CREATE OR REPLACE TABLE SBI_STAGING.stg_theme_master (
    theme_hk BINARY(64) COMMENT 'テーマハッシュキー',
    theme_hashdiff BINARY(64) COMMENT '属性ハッシュ差分',
    theme_id VARCHAR(10) COMMENT 'テーマID',
    theme_regist_month VARCHAR(6) COMMENT 'テーマ登録月 (YYYYMM)',
    theme_name VARCHAR(512) COMMENT 'テーマ名',
    target_date DATE COMMENT 'データ対象日',
    load_date TIMESTAMP_NTZ COMMENT 'ロード日時',
    record_source VARCHAR(100) COMMENT 'レコードソース' -- エラー回避のため拡張
) COMMENT = '投資テーママスタ：ソースデータ格納用';

-- テストデータの挿入（100件程度を生成する例）
TRUNCATE TABLE SBI_STAGING.stg_theme_master;

INSERT INTO SBI_STAGING.stg_theme_master
SELECT 
    -- 約定側の SHA2_BINARY(coalesce(theme_id,''), 256) と完全に一致させる
    SHA2_BINARY(coalesce(theme_id, ''), 256) as theme_hk,
    SHA2_BINARY(concat_ws('|', coalesce(theme_regist_month, ''), coalesce(theme_name, '')), 256) as theme_hashdiff,
    theme_id,
    theme_regist_month,
    theme_name,
    CURRENT_DATE(),
    CURRENT_TIMESTAMP(),
    'TEST_GEN_THEME_MASTER'
FROM (
    -- 約定データ(stg_st_sec_test)に存在するテーマIDを優先して生成
    SELECT DISTINCT theme_id, '202510' as theme_regist_month, 'テストテーマ_' || theme_id as theme_name 
    FROM SBI_STAGING.stg_st_sec_test 
    WHERE theme_id IS NOT NULL
);