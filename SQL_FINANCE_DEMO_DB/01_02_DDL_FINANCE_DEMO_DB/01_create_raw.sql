USE ROLE FINANCE_ADMIN_ROLE;
USE DATABASE FINANCE_DEMO_DB;
USE WAREHOUSE FINANCE_DEMO_WH;
USE SCHEMA RAW;


-- 【現物】取引（注文）明細
CREATE OR REPLACE TABLE STOCK_CASH_ORDERS (
    order_id VARCHAR COMMENT '注文ID',
    account_id VARCHAR COMMENT '口座ID',
    brand_cd VARCHAR COMMENT '銘柄コード',
    order_type VARCHAR COMMENT '注文区分（BUY:買 / SELL:売）',
    order_quantity NUMBER COMMENT '注文数量',
    order_price NUMBER COMMENT '注文価格（指値/成行）',
    order_status VARCHAR COMMENT '注文ステータス（COMPLETED/CANCELLED/OPEN）',
    ordered_at TIMESTAMP COMMENT '注文日時'
);

-- 【現物】約定明細
CREATE OR REPLACE TABLE STOCK_CASH_EXECUTIONS (
    execution_id VARCHAR COMMENT '約定ID',
    order_id VARCHAR COMMENT '注文ID',
    account_id VARCHAR COMMENT '口座ID',
    brand_cd VARCHAR COMMENT '銘柄コード',
    trade_type VARCHAR COMMENT '売買区分（BUY:買 / SELL:売）',
    quantity NUMBER COMMENT '約定数量',
    price NUMBER COMMENT '約定価格',
    commission NUMBER COMMENT '手数料',
    executed_at TIMESTAMP COMMENT '約定日時（タイムスタンプ）',
    execution_date DATE COMMENT '約定日（日付型）' -- 追加
);


-- ※他のテーブル（注文・預り・顧客・外国株）は前回の定義通り作成してください。
-- 【信用】取引（注文）明細
CREATE OR REPLACE TABLE STOCK_MARGIN_ORDERS (
    order_id VARCHAR COMMENT '注文ID',
    account_id VARCHAR COMMENT '口座ID',
    brand_cd VARCHAR COMMENT '銘柄コード',
    order_type VARCHAR COMMENT '注文区分（NEW_OPEN:新規 / CLOSE_REPAY:返済）',
    margin_type VARCHAR COMMENT '信用区分（SYSTEM:制度 / GENERAL:一般）',
    order_quantity NUMBER COMMENT '注文数量',
    order_price NUMBER COMMENT '注文価格',
    ordered_at TIMESTAMP COMMENT '注文日時'
);

-- 【信用】約定明細
CREATE OR REPLACE TABLE STOCK_MARGIN_EXECUTIONS (
    execution_id VARCHAR COMMENT '約定ID',
    order_id VARCHAR COMMENT '注文ID',
    account_id VARCHAR COMMENT '口座ID',
    brand_cd VARCHAR COMMENT '銘柄コード',
    trade_type VARCHAR COMMENT '売買区分',
    margin_type VARCHAR COMMENT '信用区分',
    quantity NUMBER COMMENT '約定数量',
    price NUMBER COMMENT '約定価格',
    interest_rate FLOAT COMMENT '金利',
    executed_at TIMESTAMP COMMENT '約定日時（タイムスタンプ）',
    execution_date DATE COMMENT '約定日（日付型）' -- 追加
);

-- 【現物】預り明細
CREATE OR REPLACE TABLE STOCK_CASH_HOLDINGS (
    account_id VARCHAR COMMENT '口座ID',
    brand_cd VARCHAR COMMENT '銘柄コード',
    quantity NUMBER COMMENT '保有数量',
    average_cost NUMBER COMMENT '取得単価（平均コスト）',
    as_of_date DATE COMMENT '基準日'
);

-- 【信用】預り明細（建玉明細）
CREATE OR REPLACE TABLE STOCK_MARGIN_HOLDINGS (
    account_id VARCHAR COMMENT '口座ID',
    brand_cd VARCHAR COMMENT '銘柄コード',
    side VARCHAR COMMENT '建玉区分（LONG:買建 / SHORT:売建）',
    quantity NUMBER COMMENT '建玉数量',
    open_price NUMBER COMMENT '建単価',
    current_price NUMBER COMMENT '現在値',
    evaluation_profit_loss NUMBER COMMENT '評価損益',
    as_of_date DATE COMMENT '基準日'
);

-- 【外国株】取引（注文）明細
CREATE OR REPLACE TABLE FOREIGN_STOCK_ORDERS (
    order_id VARCHAR COMMENT '注文ID',
    account_id VARCHAR COMMENT '口座ID',
    brand_cd VARCHAR COMMENT '銘柄コード',
    market_code VARCHAR COMMENT '市場コード（US/HK/EU等）',
    currency_code VARCHAR COMMENT '通貨コード（USD/HKD等）',
    order_quantity NUMBER COMMENT '注文数量',
    order_price NUMBER(18, 2) COMMENT '注文価格',
    ordered_at TIMESTAMP COMMENT '注文日時'
);

-- 【外国株】約定明細
CREATE OR REPLACE TABLE FOREIGN_STOCK_EXECUTIONS (
    execution_id VARCHAR COMMENT '約定ID',
    order_id VARCHAR COMMENT '注文ID',
    account_id VARCHAR COMMENT '口座ID',
    brand_cd VARCHAR COMMENT '銘柄コード',
    market_code VARCHAR COMMENT '市場コード（US/HK/EU等）',
    currency_code VARCHAR COMMENT '通貨コード（USD/HKD等）',
    trade_type VARCHAR COMMENT '売買区分（BUY:買 / SELL:売）',
    quantity NUMBER COMMENT '約定数量',
    price NUMBER(18, 2) COMMENT '約定価格',
    commission NUMBER(18, 2) COMMENT '手数料',
    executed_at TIMESTAMP COMMENT '約定日時（タイムスタンプ）',
    execution_date DATE COMMENT '約定日（日付型）'
);

-- 【外国株】預り明細
CREATE OR REPLACE TABLE FOREIGN_STOCK_HOLDINGS (
    account_id VARCHAR COMMENT '口座ID',
    brand_cd VARCHAR COMMENT '銘柄コード',
    market_code VARCHAR COMMENT '市場コード',
    currency_code VARCHAR COMMENT '通貨コード',
    quantity NUMBER COMMENT '保有数量',
    average_cost NUMBER(18, 2) COMMENT '取得単価（現地通貨ベース平均コスト）',
    as_of_date DATE COMMENT '基準日'
);


-- 【共通】顧客口座情報
CREATE OR REPLACE TABLE CUSTOMER_PROFILES (
    account_id VARCHAR COMMENT '口座ID',
    customer_name VARCHAR COMMENT '顧客名',
    customer_type VARCHAR COMMENT '顧客区分（個人/法人）',
    address VARCHAR COMMENT '住所',
    segment VARCHAR COMMENT '顧客セグメント',
    updated_at TIMESTAMP COMMENT '情報更新日時'
);

-- 【共通】入出金明細
CREATE OR REPLACE TABLE RAW.CASH_TRANSACTIONS (
    transaction_id VARCHAR COMMENT '入出金ID',
    account_id VARCHAR COMMENT '口座ID',
    currency_code VARCHAR COMMENT '通貨コード（JPY/USD等）',
    transaction_type VARCHAR COMMENT '区分（DEPOSIT:入金 / WITHDRAW:出金 / DIVIDEND:配当金）',
    amount NUMBER(18, 2) COMMENT '金額',
    description VARCHAR COMMENT '摘要（振込、ATM、配当等）',
    executed_at TIMESTAMP COMMENT '執行日時',
    execution_date DATE COMMENT '執行日'
);

-- 【マスタ】銘柄マスタ
CREATE OR REPLACE TABLE RAW.STOCK_MASTER (
    brand_cd VARCHAR COMMENT '銘柄コード',
    brand_name VARCHAR COMMENT '銘柄名',
    market_name VARCHAR COMMENT '市場名（東証プライム、ナスダック等）',
    sector_code VARCHAR COMMENT '業種コード',
    sector_name VARCHAR COMMENT '業種名（情報・通信、電気機器等）',
    country_code VARCHAR COMMENT '国コード（JP, US等）',
    is_active BOOLEAN COMMENT '有効フラグ',
    updated_at TIMESTAMP COMMENT 'マスタ更新日時'
);


-- 【市場】為替レートマスタ
CREATE OR REPLACE TABLE RAW.EXCHANGE_RATES (
    currency_pair VARCHAR COMMENT '通貨ペア（USD/JPY, HKD/JPY等）',
    base_currency VARCHAR COMMENT '元通貨（USD等）',
    target_currency VARCHAR COMMENT '対象通貨（JPY等）',
    tts_rate NUMBER(18, 4) COMMENT '対顧客電信売相場（購入時レート）',
    ttb_rate NUMBER(18, 4) COMMENT '対顧客電信買相場（売却時レート）',
    ttm_rate NUMBER(18, 4) COMMENT '仲値（評価用基準レート）',
    as_of_date DATE COMMENT '適用日',
    updated_at TIMESTAMP COMMENT 'データ取得日時'
);

-- 【市場】日次時価データ
CREATE OR REPLACE TABLE RAW.STOCK_PRICES (
    brand_cd VARCHAR COMMENT '銘柄コード',
    market_code VARCHAR COMMENT '市場コード',
    currency_code VARCHAR COMMENT '通貨コード',
    close_price NUMBER(18, 2) COMMENT '終値',
    high_price NUMBER(18, 2) COMMENT '高値',
    low_price NUMBER(18, 2) COMMENT '安値',
    open_price NUMBER(18, 2) COMMENT '始値',
    trading_volume NUMBER COMMENT '出来高',
    as_of_date DATE COMMENT '時価基準日',
    updated_at TIMESTAMP COMMENT 'データ取得日時'
);