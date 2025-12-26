USE ROLE FINANCE_ADMIN_ROLE;
USE DATABASE FINANCE_DEMO_DB;
USE WAREHOUSE FINANCE_DEMO_WH;
USE SCHEMA RAW;

-- 1. 約定明細 (Trade Executions)
-- いつ、どの銘柄を、いくらで、何株売買したか
CREATE OR REPLACE TABLE RAW.EXECUTIONS (
    EXECUTION_ID STRING,
    ACCOUNT_ID STRING,
    SYMBOL STRING,
    SIDE STRING, -- 'BUY' or 'SELL'
    QUANTITY NUMBER(18,4),
    PRICE NUMBER(18,4),
    EXECUTION_AT TIMESTAMP_NTZ,
    COMMISSION NUMBER(18,2)
);

-- 2. 取引明細 (Account Transactions)
-- 入出金や配当金など、現金の動き
CREATE OR REPLACE TABLE RAW.TRANSACTIONS (
    TRANSACTION_ID STRING,
    ACCOUNT_ID STRING,
    TRANSACTION_TYPE STRING, -- 'DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'TRADE'
    AMOUNT NUMBER(18,2),
    CURRENCY STRING,
    TRANSACTION_DATE DATE
);

-- 3. 預り明細 (Position/Balances)
-- 各アカウントが保有している銘柄の残高情報（スナップショット想定）
CREATE OR REPLACE TABLE RAW.POSITIONS (
    SNAPSHOT_DATE DATE,
    ACCOUNT_ID STRING,
    SYMBOL STRING,
    QUANTITY NUMBER(18,4),
    AVERAGE_COST NUMBER(18,4)
);


-- 【現物】取引（注文）明細
CREATE OR REPLACE TABLE RAW.STOCK_CASH_ORDERS (
    order_id VARCHAR, account_id VARCHAR, 
    stock_symbol VARCHAR, 
    order_type VARCHAR, -- BUY/SELL
    order_quantity NUMBER, order_price NUMBER, -- 指値/成行
    order_status VARCHAR, -- COMPLETED/CANCELLED/OPEN
    ordered_at TIMESTAMP
);

-- 【現物】約定明細
CREATE OR REPLACE TABLE RAW.STOCK_CASH_EXECUTIONS (
    execution_id VARCHAR, 
    order_id VARCHAR, 
    account_id VARCHAR, 
    stock_symbol VARCHAR, 
    trade_type VARCHAR, -- BUY/SELL
    quantity NUMBER, 
    price NUMBER, 
    commission NUMBER, 
    executed_at TIMESTAMP
);

-- 【信用】取引（注文）明細
CREATE OR REPLACE TABLE RAW.STOCK_MARGIN_ORDERS (
    order_id VARCHAR, account_id VARCHAR, stock_symbol VARCHAR, 
    order_type VARCHAR, -- NEW_OPEN/CLOSE_REPAY
    margin_type VARCHAR, -- SYSTEM/GENERAL
    order_quantity NUMBER, order_price NUMBER, 
    ordered_at TIMESTAMP
);

-- 【信用】約定明細
CREATE OR REPLACE TABLE RAW.STOCK_MARGIN_EXECUTIONS (
    execution_id VARCHAR, order_id VARCHAR, account_id VARCHAR, stock_symbol VARCHAR, 
    trade_type VARCHAR, margin_type VARCHAR,
    quantity NUMBER, price NUMBER, interest_rate FLOAT, 
    executed_at TIMESTAMP
);

-- 【現物】預り明細
CREATE OR REPLACE TABLE RAW.STOCK_CASH_HOLDINGS (
    account_id VARCHAR, stock_symbol VARCHAR, 
    quantity NUMBER, average_cost NUMBER, 
    as_of_date DATE
);

-- 【信用】預り明細（建玉明細）
CREATE OR REPLACE TABLE RAW.STOCK_MARGIN_HOLDINGS (
    account_id VARCHAR, stock_symbol VARCHAR, 
    side VARCHAR, -- LONG(買建)/SHORT(売建)
    quantity NUMBER, open_price NUMBER, 
    current_price NUMBER, evaluation_profit_loss NUMBER,
    as_of_date DATE
);