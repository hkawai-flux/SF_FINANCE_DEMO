import pandas as pd
import numpy as np
from faker import Faker
from snowflake.snowpark import Session
from datetime import datetime, timedelta

# --- 初期設定 ---
fake = Faker('ja_JP')
Faker.seed(42)
session = Session.builder.configs({
    "account": "<account>", "user": "<user>", "password": "<password>",
    "role": "FINANCE_ADMIN_ROLE", "warehouse": "FINANCE_DEMO_WH",
    "database": "FINANCE_DEMO_DB", "schema": "RAW"
}).create()

num_customers = 500
num_rows = 5000
base_date = datetime(2025, 12, 26)

# --- 共通マスタ準備 ---
acc_ids = [f'ACC{i:05}' for i in range(1, num_customers + 1)]
brand_list = [f'{1000 + i}' for i in range(100)] # 100銘柄

# --- 1. 顧客情報 & 銘柄マスタ ---
df_cust = pd.DataFrame([{
    'account_id': aid, 'customer_name': fake.name(), 
    'customer_type': np.random.choice(['個人', '法人']),
    'address': fake.address(), 'segment': np.random.choice(['マス', '富裕層']),
    'updated_at': base_date} for aid in acc_ids])

df_stock_master = pd.DataFrame([{
    'brand_cd': b, 'brand_name': f'デモ銘柄_{b}', 'market_name': '東証プライム',
    'sector_name': '情報・通信', 'country_code': 'JP', 'is_active': True,
    'updated_at': base_date} for b in brand_list])

# --- 2. 取引・約定・預り（国内・外国） ---
# (中略: 同様のロジックで全テーブルのDataFrameを作成)
# 例: 現物約定
df_cash_exec = pd.DataFrame([{
    'execution_id': f'EX{i:07}', 'order_id': f'OR{i:07}', 'account_id': np.random.choice(acc_ids),
    'brand_cd': np.random.choice(brand_list), 'trade_type': 'BUY', 'quantity': 100,
    'price': 1500, 'commission': 50, 'executed_at': base_date + timedelta(hours=1),
    'execution_date': base_date.date()} for i in range(num_rows)])

# --- Snowflakeへ一括ロード ---
tables = {
    "CUSTOMER_PROFILES": df_cust,
    "STOCK_MASTER": df_stock_master,
    "STOCK_CASH_EXECUTIONS": df_cash_exec,
    # ... 他のテーブルも同様に session.write_pandas する
}

for table_name, df in tables.items():
    session.write_pandas(df, table_name, auto_create_table=False, overwrite=True)
    print(f"Loaded {len(df)} rows into {table_name}")

session.close()