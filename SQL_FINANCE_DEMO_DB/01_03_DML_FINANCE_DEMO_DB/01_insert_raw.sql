

USE ROLE FINANCE_ADMIN_ROLE;
USE DATABASE FINANCE_DEMO_DB;
USE WAREHOUSE FINANCE_DEMO_WH;
USE SCHEMA RAW;

--国内株データ作成用SQL概要
--国内株の注文、約定、預り明細のテストデータを大量生成するためのSQL案です。 Snowflakeの `GENERATOR` 関数を使用して、5,000〜10,000レコードの擬似データを効率的に作成します。

--1. 取引（注文）明細: STOCK_CASH_ORDERS (10,000件)
INSERT INTO RAW.STOCK_CASH_ORDERS
SELECT
    'ORD' || LPAD(SEQ4(), 7, '0') AS order_id,
    'ACC' || LPAD(UNIFORM(1, 1000, RANDOM()), 5, '0') AS account_id,
    (1000 + UNIFORM(1, 100, RANDOM()))::VARCHAR AS brand_cd,
    CASE WHEN RANDOM() % 2 = 0 THEN 'BUY' ELSE 'SELL' END AS order_type,
    UNIFORM(100, 1000, RANDOM()) AS order_quantity,
    UNIFORM(500, 10000, RANDOM()) AS order_price,
    CASE 
        WHEN RANDOM() % 10 < 7 THEN 'COMPLETED'
        WHEN RANDOM() % 10 < 9 THEN 'CANCELLED'
        ELSE 'OPEN'
    END AS order_status,
    DATEADD(second, UNIFORM(0, 86400, RANDOM()), '2025-12-26 09:00:00'::TIMESTAMP) AS ordered_at
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

--2. 約定明細: STOCK_CASH_EXECUTIONS (8,000件)
--注文データの一部が約定したと仮定して作成します。
INSERT INTO RAW.STOCK_CASH_EXECUTIONS
SELECT
    'EXE' || LPAD(SEQ4(), 7, '0') AS execution_id,
    'ORD' || LPAD(SEQ4(), 7, '0') AS order_id,
    'ACC' || LPAD(UNIFORM(1, 1000, RANDOM()), 5, '0') AS account_id,
    (1000 + UNIFORM(1, 100, RANDOM()))::VARCHAR AS brand_cd,
    CASE WHEN RANDOM() % 2 = 0 THEN 'BUY' ELSE 'SELL' END AS trade_type,
    UNIFORM(100, 1000, RANDOM()) AS quantity,
    UNIFORM(500, 10000, RANDOM()) AS price,
    UNIFORM(50, 500, RANDOM()) AS commission,
    DATEADD(minute, UNIFORM(1, 60, RANDOM()), '2025-12-26 10:00:00'::TIMESTAMP) AS executed_at,
    '2025-12-26'::DATE AS execution_date
FROM TABLE(GENERATOR(ROWCOUNT => 8000));

--3. 預り明細: STOCK_CASH_HOLDINGS (5,000件)
INSERT INTO RAW.STOCK_CASH_HOLDINGS
SELECT
    'ACC' || LPAD(UNIFORM(1, 1000, RANDOM()), 5, '0') AS account_id,
    (1000 + UNIFORM(1, 100, RANDOM()))::VARCHAR AS brand_cd,
    UNIFORM(100, 5000, RANDOM()) AS quantity,
    UNIFORM(500, 8000, RANDOM()) AS average_cost,
    '2025-12-26'::DATE AS as_of_date
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

--1. 信用取引（注文）明細: STOCK_MARGIN_ORDERS (5,000件)
INSERT INTO RAW.STOCK_MARGIN_ORDERS
SELECT
    'M_ORD' || LPAD(SEQ4(), 7, '0') AS order_id,
    'ACC' || LPAD(UNIFORM(1, 1000, RANDOM()), 5, '0') AS account_id,
    (1000 + UNIFORM(1, 100, RANDOM()))::VARCHAR AS brand_cd,
    CASE WHEN RANDOM() % 2 = 0 THEN 'NEW_OPEN' ELSE 'CLOSE_REPAY' END AS order_type,
    CASE WHEN RANDOM() % 2 = 0 THEN 'SYSTEM' ELSE 'GENERAL' END AS margin_type,
    UNIFORM(100, 1000, RANDOM()) AS order_quantity,
    UNIFORM(500, 10000, RANDOM()) AS order_price,
    DATEADD(second, UNIFORM(0, 86400, RANDOM()), '2025-12-26 09:00:00'::TIMESTAMP) AS ordered_at
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

--2. 信用取引 約定明細: STOCK_MARGIN_EXECUTIONS (4,000件)

INSERT INTO RAW.STOCK_MARGIN_EXECUTIONS
SELECT
    'M_EXE' || LPAD(SEQ4(), 7, '0') AS execution_id,
    'M_ORD' || LPAD(SEQ4(), 7, '0') AS order_id,
    'ACC' || LPAD(UNIFORM(1, 1000, RANDOM()), 5, '0') AS account_id,
    (1000 + UNIFORM(1, 100, RANDOM()))::VARCHAR AS brand_cd,
    CASE WHEN RANDOM() % 2 = 0 THEN 'BUY' ELSE 'SELL' END AS trade_type,
    CASE WHEN RANDOM() % 2 = 0 THEN 'SYSTEM' ELSE 'GENERAL' END AS margin_type,
    UNIFORM(100, 1000, RANDOM()) AS quantity,
    UNIFORM(500, 10000, RANDOM()) AS price,
    0.025 AS interest_rate, -- 金利一律 2.5%
    DATEADD(minute, UNIFORM(1, 60, RANDOM()), '2025-12-26 10:00:00'::TIMESTAMP) AS executed_at,
    '2025-12-26'::DATE AS execution_date
FROM TABLE(GENERATOR(ROWCOUNT => 4000));

--3. 信用預り（建玉）明細: STOCK_MARGIN_HOLDINGS (3,000件)
INSERT INTO RAW.STOCK_MARGIN_HOLDINGS
SELECT
    'ACC' || LPAD(UNIFORM(1, 1000, RANDOM()), 5, '0') AS account_id,
    (1000 + UNIFORM(1, 100, RANDOM()))::VARCHAR AS brand_cd,
    CASE WHEN RANDOM() % 2 = 0 THEN 'LONG' ELSE 'SHORT' END AS side,
    UNIFORM(100, 5000, RANDOM()) AS quantity,
    UNIFORM(500, 8000, RANDOM()) AS open_price,
    UNIFORM(500, 8000, RANDOM()) AS current_price,
    UNIFORM(-100000, 100000, RANDOM()) AS evaluation_profit_loss,
    '2025-12-26'::DATE AS as_of_date
FROM TABLE(GENERATOR(ROWCOUNT => 3000));



--1. 外国株（注文）明細: FOREIGN_STOCK_ORDERS (3,000件)
INSERT INTO RAW.FOREIGN_STOCK_ORDERS
SELECT
    'F_ORD' || LPAD(SEQ4(), 7, '0') AS order_id,
    'ACC' || LPAD(UNIFORM(1, 1000, RANDOM()), 5, '0') AS account_id,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'AAPL' WHEN 2 THEN 'TSLA' WHEN 3 THEN 'NVDA' ELSE 'MSFT'
    END AS brand_cd,
    'US' AS market_code,
    'USD' AS currency_code,
    UNIFORM(1, 500, RANDOM()) AS order_quantity,
    (UNIFORM(100, 500, RANDOM()) + (ABS(RANDOM()) % 100 / 100.0))::NUMBER(18,2) AS order_price,
    DATEADD(second, UNIFORM(0, 86400, RANDOM()), '2025-12-26 09:00:00'::TIMESTAMP) AS ordered_at
FROM TABLE(GENERATOR(ROWCOUNT => 3000));

--2. 外国株 約定明細: FOREIGN_STOCK_EXECUTIONS (2,500件)
INSERT INTO RAW.FOREIGN_STOCK_EXECUTIONS
SELECT
    'F_EXE' || LPAD(SEQ4(), 7, '0') AS execution_id,
    'F_ORD' || LPAD(SEQ4(), 7, '0') AS order_id,
    'ACC' || LPAD(UNIFORM(1, 1000, RANDOM()), 5, '0') AS account_id,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'AAPL' WHEN 2 THEN 'TSLA' WHEN 3 THEN 'NVDA' ELSE 'MSFT'
    END AS brand_cd,
    'US' AS market_code,
    'USD' AS currency_code,
    CASE WHEN RANDOM() % 2 = 0 THEN 'BUY' ELSE 'SELL' END AS trade_type,
    UNIFORM(1, 500, RANDOM()) AS quantity,
    (UNIFORM(100, 500, RANDOM()) + (ABS(RANDOM()) % 100 / 100.0))::NUMBER(18,2) AS price,
    (UNIFORM(1, 10, RANDOM()) + 0.99)::NUMBER(18,2) AS commission,
    DATEADD(minute, UNIFORM(1, 60, RANDOM()), '2025-12-26 10:00:00'::TIMESTAMP) AS executed_at,
    '2025-12-26'::DATE AS execution_date
FROM TABLE(GENERATOR(ROWCOUNT => 2500));

--3. 外国株 預り明細: FOREIGN_STOCK_HOLDINGS (2,000件)
INSERT INTO RAW.FOREIGN_STOCK_HOLDINGS
SELECT
    'ACC' || LPAD(UNIFORM(1, 1000, RANDOM()), 5, '0') AS account_id,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'AAPL' WHEN 2 THEN 'TSLA' WHEN 3 THEN 'NVDA' ELSE 'MSFT'
    END AS brand_cd,
    'US' AS market_code,
    'USD' AS currency_code,
    UNIFORM(1, 2000, RANDOM()) AS quantity,
    (UNIFORM(100, 450, RANDOM()) + (ABS(RANDOM()) % 100 / 100.0))::NUMBER(18,2) AS average_cost,
    '2025-12-26'::DATE AS as_of_date
FROM TABLE(GENERATOR(ROWCOUNT => 2000));

--1. 顧客口座情報: CUSTOMER_PROFILES (5,000件)
INSERT INTO CUSTOMER_PROFILES
SELECT
    'ACC' || LPAD(SEQ4(), 5, '0') AS account_id,
    '顧客名_' || SEQ4() AS customer_name,
    CASE WHEN RANDOM() % 2 = 0 THEN '個人' ELSE '法人' END AS customer_type,
    '住所_' || SEQ4() AS address,
    CASE WHEN RANDOM() % 10 < 2 THEN '富裕層' ELSE 'マス' END AS segment,
    '2025-12-26 10:00:00'::TIMESTAMP AS updated_at
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

--2. 入出金明細: CASH_TRANSACTIONS (10,000件)
INSERT INTO CASH_TRANSACTIONS
SELECT
    'TR' || LPAD(SEQ4(), 8, '0') AS transaction_id,
    'ACC' || LPAD(UNIFORM(1, 5000, RANDOM()), 5, '0') AS account_id,
    'JPY' AS currency_code,
    CASE WHEN RANDOM() % 10 < 7 THEN 'DEPOSIT' ELSE 'WITHDRAW' END AS transaction_type,
    UNIFORM(1000, 1000000, RANDOM()) AS amount,
    'デモ入出金' AS description,
    DATEADD(second, UNIFORM(0, 86400, RANDOM()), '2025-12-26 09:00:00'::TIMESTAMP) AS executed_at,
    '2025-12-26'::DATE AS execution_date
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

--3. 銘柄マスタ: STOCK_MASTER (5,000件)
INSERT INTO STOCK_MASTER
SELECT
    (UNIFORM(1000, 9999, RANDOM()))::VARCHAR AS brand_cd,
    'デモ銘柄_' || SEQ4() AS brand_name,
    CASE WHEN RANDOM() % 3 = 0 THEN '東証プライム' WHEN RANDOM() % 3 = 1 THEN '東証グロース' ELSE 'NASDAQ' END AS market_name,
    '10' AS sector_code,
    CASE WHEN RANDOM() % 2 = 0 THEN '情報・通信' ELSE '製造業' END AS sector_name,
    CASE WHEN RANDOM() % 3 = 0 THEN 'JP' ELSE 'US' END AS country_code,
    TRUE AS is_active,
    '2025-12-26 10:00:00'::TIMESTAMP AS updated_at
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

--1. 為替レートマスタ: EXCHANGE_RATES (5,000件)
INSERT INTO EXCHANGE_RATES
SELECT
    'USD/JPY' AS currency_pair,
    'USD' AS base_currency,
    'JPY' AS target_currency,
    (145.00 + (ABS(RANDOM()) % 1000 / 100.0))::NUMBER(18,4) AS tts_rate, -- 145〜155円程度で変動
    (143.00 + (ABS(RANDOM()) % 1000 / 100.0))::NUMBER(18,4) AS ttb_rate,
    (144.00 + (ABS(RANDOM()) % 1000 / 100.0))::NUMBER(18,4) AS ttm_rate,
    DATEADD(day, - (SEQ4()), '2025-12-26')::DATE AS as_of_date, -- 本日から過去5000日分
    CURRENT_TIMESTAMP() AS updated_at
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

--2. 日次時価データ: STOCK_PRICES (10,000件)
INSERT INTO STOCK_PRICES
SELECT
    -- 先ほど作成した銘柄マスタや約定データにあるコードをランダムに割り当て
    CASE WHEN RANDOM() % 2 = 0 
         THEN (UNIFORM(1000, 9999, RANDOM()))::VARCHAR -- 国内株
         ELSE CASE UNIFORM(1, 5, RANDOM())
                WHEN 1 THEN 'AAPL' WHEN 2 THEN 'TSLA' WHEN 3 THEN 'NVDA' WHEN 4 THEN 'MSFT' ELSE 'GOOGL'
              END -- 外国株
    END AS brand_cd,
    CASE WHEN brand_cd REGEXP '[0-9]+' THEN 'JP' ELSE 'US' END AS market_code,
    CASE WHEN market_code = 'JP' THEN 'JPY' ELSE 'USD' END AS currency_code,
    CASE WHEN market_code = 'JP' 
         THEN UNIFORM(500, 10000, RANDOM()) 
         ELSE (UNIFORM(100, 800, RANDOM()) + (ABS(RANDOM()) % 100 / 100.0)) 
    END AS close_price,
    close_price * 1.02 AS high_price,
    close_price * 0.98 AS low_price,
    close_price AS open_price,
    UNIFORM(1000, 1000000, RANDOM()) AS trading_volume,
    DATEADD(day, - (UNIFORM(0, 365, RANDOM())), '2025-12-26')::DATE AS as_of_date,
    CURRENT_TIMESTAMP() AS updated_at
FROM TABLE(GENERATOR(ROWCOUNT => 10000));


--1. 外国株信用 約定デモデータ挿入 (10,000件)
INSERT INTO RAW.FOREIGN_MARGIN_EXECUTIONS (EXECUTION_ID, ACCOUNT_ID, BRAND_CD, SIDE, EXECUTION_QTY, EXECUTION_PRICE_LOCAL, EXECUTION_AMOUNT_JPY, EXECUTION_TIMESTAMP)
SELECT
    'EX_FOR_' || UUID_STRING(),        -- ユニークな約定ID
    'ACC_' || LPAD(UNIFORM(1, 2000, RANDOM()), 5, '0'),
    CASE UNIFORM(1, 5, RANDOM()) 
        WHEN 1 THEN 'AAPL' WHEN 2 THEN 'TSLA' WHEN 3 THEN 'NVDA' WHEN 4 THEN 'MSFT' ELSE 'GOOGL' 
    END,
    CASE WHEN RANDOM() > 0 THEN 'BUY' ELSE 'SELL' END,
    UNIFORM(1, 100, RANDOM()),
    UNIFORM(100, 300, RANDOM()),
    UNIFORM(15000, 3000000, RANDOM()), -- 代金
    DATEADD(SECOND, UNIFORM(0, 86400, RANDOM()), '2025-12-28 00:00:00') -- 12/28の24時間に分散
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

--2. 外国株信用 建玉（残高）デモデータ挿入 (8,000件)
INSERT INTO RAW.FOREIGN_MARGIN_HOLDINGS (ACCOUNT_ID, BRAND_CD, SIDE, QUANTITY, OPEN_PRICE_LOCAL, OPEN_PRICE_JPY, INTEREST_JPY)
SELECT
    'ACC_' || LPAD(UNIFORM(1, 2000, RANDOM()), 5, '0'), -- 2000口座に分散
    CASE UNIFORM(1, 5, RANDOM()) 
        WHEN 1 THEN 'AAPL' WHEN 2 THEN 'TSLA' WHEN 3 THEN 'NVDA' WHEN 4 THEN 'MSFT' ELSE 'GOOGL' 
    END,
    CASE WHEN RANDOM() > 0 THEN 'BUY' ELSE 'SELL' END,
    UNIFORM(1, 500, RANDOM()),         -- 数量 1-500
    UNIFORM(100, 300, RANDOM()),       -- 現地価格 100-300
    UNIFORM(15000, 45000, RANDOM()),   -- 円貨価格 15000-45000
    UNIFORM(0, 5000, RANDOM())         -- 金利 0-5000
FROM TABLE(GENERATOR(ROWCOUNT => 8000));