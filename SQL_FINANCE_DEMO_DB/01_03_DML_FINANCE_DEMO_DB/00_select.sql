USE ROLE ACCOUNTADMIN;

-- 現在のアカウント識別子（組織名.アカウント名）を表示する
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER;



USE ROLE FINANCE_ADMIN_ROLE;
USE WAREHOUSE FINANCE_DEMO_WH;
USE DATABASE FINANCE_DEMO_DB;

USE SCHEMA CORE;
--PIT TABLE - MART
-- イメージ：Mart 層での結合例
select 
    pit.snapshot_date,
    sat_cash.quantity,
    sat_margin.evaluation_profit_loss
from pit_account_holdings pit
join sat_stock_cash_holding_details sat_cash 
    on pit.account_hk = sat_cash.account_hk 
    and pit.cash_holding_load_date = sat_cash.load_date
join sat_stock_margin_holding_details sat_margin
    on pit.account_hk = sat_margin.account_hk
    and pit.margin_holding_load_date = sat_margin.load_date;

SELECT * FROM FINANCE_DEMO_DB.SEMANTIC.MART_DAILY_ACCOUNT_PERFORMANCE 
ORDER BY SNAPSHOT_DATE DESC, ACCOUNT_ID;

SELECT MIN(LOAD_DATE), MAX(LOAD_DATE) FROM FINANCE_DEMO_DB.CORE.SAT_STOCK_CASH_HOLDING_DETAILS;