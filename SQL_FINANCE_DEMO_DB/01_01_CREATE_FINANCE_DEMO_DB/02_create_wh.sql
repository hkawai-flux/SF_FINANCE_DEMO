USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TEMP_WH;

SHOW WAREHOUSES;
SHOW RESOURCE MONITORS;

-- ウェアハウスの作成
CREATE OR REPLACE WAREHOUSE FINANCE_DEMO_WH
    WAREHOUSE_SIZE = 'XSMALL'            -- サイズ (XSMALL, SMALL, MEDIUM...)
    WAREHOUSE_TYPE = 'STANDARD'          -- タイプ
    AUTO_SUSPEND = 60                    -- 1分間未使用なら自動停止（秒単位）
    AUTO_RESUME = TRUE                   -- クエリ実行時に自動で再開
    INITIALLY_SUSPENDED = TRUE           -- 作成時は停止状態にする
    COMMENT = '金融系DB_DEMO用の標準ウェアハウス';

-- リソースモニターの作成
CREATE OR REPLACE RESOURCE MONITOR FINANCE_DEMO_WH_MONITER
    CREDIT_QUOTA = 100                   -- 月間のクレジット上限（例: 100クレジット）
    FREQUENCY = 'MONTHLY'                -- リセット周期 (MONTHLY, DAILY...)
    START_TIMESTAMP = IMMEDIATELY        -- 開始タイミング
    TRIGGERS
        ON 75 PERCENT DO NOTIFY          -- 75%到達で通知
        ON 90 PERCENT DO NOTIFY          -- 90%到達で通知
        ON 100 PERCENT DO SUSPEND        -- 100%到達で新規クエリを拒否（実行中は継続）
        ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- 110%到達で実行中のクエリも強制終了
