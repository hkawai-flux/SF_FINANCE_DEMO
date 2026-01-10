-- 既に作成したBVテーブルに対し、AIへの説明を付与
ALTER TABLE SBI_BUSINESS_VAULT.BV_DAILY_EXECUTION_METRICS 
MODIFY COLUMN 
    UNIT_S_KBN COMMENT '単元株かS株（単元未満株）かを識別する区分。KPI010/007のフィルタに使用',
    DEPOSIT_KBN COMMENT '預り区分。特定, 一般, 新NISA等の値を持ち、税制優遇の分析に使用',
    EXEC_COUNT COMMENT '約定件数の合計値。注文単位でのユニークカウント済み';


-- ガバナンス用タグの作成（管理者ロールで実行）
CREATE OR REPLACE SCHEMA SBI_TAGS;
CREATE TAG IF NOT EXISTS SBI_TAGS.DATA_QUALITY_TAG;

-- テーブルに「認証済み」タグを付与（AIはこのタグがあるテーブルを優先回答します）
ALTER TABLE SBI_BUSINESS_VAULT.BV_DAILY_EXECUTION_METRICS 
SET TAG SBI_TAGS.DATA_QUALITY_TAG = 'CERTIFIED_GOLD';

--　内部ステージにyamlファイルをアップロード（管理者ロールで実行）
USE SCHEMA SEMANTIC;
put file://C:\Users\HideakiKawai\Documents\03_development\SF_FINANCE_DEMO\SQL_FINANCE_DEMO_DB\04_SBI_TEST\sbi_bisiness_vault\ddl_semantic_view\domestic_stock_analysis_model\domestic_stock_analysis_model.yaml @semantic_assets;
put file://C:\Users\HideakiKawai\Documents\03_development\SF_FINANCE_DEMO\SQL_FINANCE_DEMO_DB\04_SBI_TEST\sbi_bisiness_vault\ddl_semantic_view\domestic_stock_analysis_model\domestic_stock_analysis_model.yaml @semantic_assets OVERWRITE = TRUE;


USE ROLE ACCOUNTADMIN;
SHOW DATA METRIC FUNCTIONS IN DATABASE FINANCE_DEMO_DB;
DROP FUNCTION FINANCE_DEMO_DB.SEMANTIC.check_positive_amount(TABLE(NUMBER));




-- データ品質モニタリング
-- 数値がマイナスにならないことをチェックする
--CREATE DATA METRIC FUNCTION check_positive_amount (
--  ARG_T TABLE (AMOUNT NUMBER)
--)
--RETURNS NUMBER
--AS 'SELECT COUNT(*) FROM ARG_T WHERE AMOUNT < 0';

-- テーブルに紐付けて監視
--ALTER TABLE SBI_BUSINESS_VAULT.BV_DAILY_EXECUTION_METRICS
--ADD DATA METRIC FUNCTION semantic.check_positive_amount ON (TOTAL_EXEC_AMOUNT);