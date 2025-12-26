USE ROLE FINANCE_ADMIN_ROLE;
USE DATABASE FINANCE_DEMO_DB;
USE WAREHOUSE FINANCE_DEMO_WH;
USE SCHEMA SEMANTIC;


put file://C:\Users\HideakiKawai\Documents\03_development\SF_FINANCE_DEMO\SQL_FINANCE_DEMO_DB\03_semantic_view_model\snowflake_semantic_model.yaml @semantic_assets;
put file://C:\Users\HideakiKawai\Documents\03_development\SF_FINANCE_DEMO\SQL_FINANCE_DEMO_DB\03_semantic_view_model\account_performance_analyst.yaml @semantic_assets;