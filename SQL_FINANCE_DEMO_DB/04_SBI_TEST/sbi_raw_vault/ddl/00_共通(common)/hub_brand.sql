-- 銘柄ハブテーブル（HUB_BRAND）の定義：カラム長を拡張
CREATE OR REPLACE TABLE SBI_RAW_VAULT.HUB_BRAND (
    BRAND_HK BINARY(64) NOT NULL COMMENT '銘柄ハッシュキー（銘柄コード＋枝番等）',
    BRAND_CD VARCHAR NOT NULL COMMENT '銘柄コード（ビジネスキー）',
    LOAD_DATE TIMESTAMP_NTZ(9) COMMENT 'ロード日時', -- CREATED_ATから統一
    RECORD_SOURCE VARCHAR(100) COMMENT 'レコードソース', -- 18から100へ拡張
    
    PRIMARY KEY (BRAND_HK)
) COMMENT = '銘柄ハブ：現物・信用・マスタから全銘柄コードを一元管理';