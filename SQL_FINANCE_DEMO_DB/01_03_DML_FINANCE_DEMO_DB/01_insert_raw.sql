

USE ROLE FINANCE_ADMIN_ROLE;
USE DATABASE FINANCE_DEMO_DB;
USE WAREHOUSE FINANCE_DEMO_WH;
USE SCHEMA RAW;

---- テストデータのインサート ---
INSERT INTO RAW.EXECUTIONS VALUES
('E001', 'ACC_001', 'AAPL', 'BUY', 10, 150.00, '2023-10-01 10:00:00', 5.00),
('E002', 'ACC_001', 'TSLA', 'BUY', 5, 250.00, '2023-10-02 11:30:00', 7.50);

INSERT INTO RAW.TRANSACTIONS VALUES
('T001', 'ACC_001', 'DEPOSIT', 10000.00, 'USD', '2023-09-25'),
('T002', 'ACC_001', 'TRADE', -1505.00, 'USD', '2023-10-01');

INSERT INTO RAW.POSITIONS VALUES
('2023-10-31', 'ACC_001', 'AAPL', 10, 150.00),
('2023-10-31', 'ACC_001', 'TSLA', 5, 250.00);


-- 取引明細 (5000行)
INSERT INTO RAW.STOCK_CASH_ORDERS
SELECT 
    'ORD-C' || seq4(), 'ACC' || LPAD(uniform(1, 100, random()), 5, '0'),
    CASE uniform(1, 3, random()) WHEN 1 THEN '7203.T' WHEN 2 THEN '9984.T' ELSE '6758.T' END,
    CASE uniform(1, 2, random()) WHEN 1 THEN 'BUY' ELSE 'SELL' END,
    uniform(100, 1000, random()), uniform(1000, 8000, random()),
    'COMPLETED', dateadd(day, -uniform(1, 30, random()), current_timestamp())
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- 約定明細 (取引に紐づく形で生成)
INSERT INTO RAW.STOCK_CASH_EXECUTIONS
SELECT 
    'EXE-C' || seq4(), order_id, account_id, stock_symbol, 
    order_type, order_quantity, order_price + uniform(-10, 10, random()), 
    uniform(100, 500, random()), ordered_at
FROM RAW.STOCK_CASH_ORDERS;

-- 取引明細 (3000行)
INSERT INTO RAW.STOCK_MARGIN_ORDERS
SELECT 
    'ORD-M' || seq4(), 'ACC' || LPAD(uniform(1, 100, random()), 5, '0'),
    CASE uniform(1, 3, random()) WHEN 1 THEN '8306.T' WHEN 2 THEN '9101.T' ELSE '4063.T' END,
    CASE uniform(1, 2, random()) WHEN 1 THEN 'NEW_OPEN' ELSE 'CLOSE_REPAY' END,
    CASE uniform(1, 2, random()) WHEN 1 THEN 'SYSTEM' ELSE 'GENERAL' END,
    uniform(100, 2000, random()), uniform(500, 4000, random()),
    dateadd(day, -uniform(1, 30, random()), current_timestamp())
FROM TABLE(GENERATOR(ROWCOUNT => 3000));

-- 約定明細
INSERT INTO RAW.STOCK_MARGIN_EXECUTIONS
SELECT 
    'EXE-M' || seq4(), order_id, account_id, stock_symbol, 
    order_type, margin_type, order_quantity, order_price, 2.8, ordered_at
FROM RAW.STOCK_MARGIN_ORDERS;

-- 現物預り
INSERT INTO RAW.STOCK_CASH_HOLDINGS
SELECT 
    'ACC' || LPAD(uniform(1, 100, random()), 5, '0'),
    CASE uniform(1, 3, random()) WHEN 1 THEN '7203.T' WHEN 2 THEN '9984.T' ELSE '6758.T' END,
    uniform(100, 5000, random()), uniform(1000, 5000, random()), current_date()
FROM TABLE(GENERATOR(ROWCOUNT => 2000));

-- 信用預り (建玉)
INSERT INTO RAW.STOCK_MARGIN_HOLDINGS
SELECT 
    'ACC' || LPAD(uniform(1, 100, random()), 5, '0'),
    CASE uniform(1, 3, random()) WHEN 1 THEN '8306.T' WHEN 2 THEN '9101.T' ELSE '4063.T' END,
    CASE uniform(1, 2, random()) WHEN 1 THEN 'LONG' ELSE 'SHORT' END,
    uniform(100, 3000, random()), uniform(1000, 3000, random()),
    uniform(1000, 3000, random()), uniform(-10000, 10000, random()), current_date()
FROM TABLE(GENERATOR(ROWCOUNT => 1000));