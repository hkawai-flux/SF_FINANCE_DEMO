-- 銘柄詳細属性（外国株）サテライト
CREATE OR REPLACE TABLE SBI_RAW_VAULT.SAT_FOREIGN_BRAND_DETAIL (
    BRAND_HK BINARY(64) NOT NULL COMMENT '銘柄ハッシュキー（HUB_BRANDと結合）',
    BRAND_HASHDIFF BINARY(64) NOT NULL COMMENT '属性ハッシュ差分（セクターや国コードの変更検知用）',
    LOAD_DATE TIMESTAMP_NTZ(9) NOT NULL COMMENT 'ロード日時（レコードの有効開始日時）',
    RECORD_SOURCE VARCHAR(100) COMMENT 'レコードソース（マスタソース名）',
    SECTOR VARCHAR(50) COMMENT 'セクター区分（米国ETF判定に使用）',
    COUNTRY_CODE VARCHAR(10) COMMENT '証券発行国コード（US:米国など）',
    
    PRIMARY KEY (BRAND_HK, LOAD_DATE)
) COMMENT = '銘柄サテライト：外国株式のセクター区分や国コードなどの属性情報を管理';

