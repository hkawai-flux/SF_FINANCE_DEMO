# SF_FINANCE_DEMO 環境全体像

本ドキュメントは、SF_FINANCE_DEMO プロジェクトのディレクトリ構成・dbt 設定・Snowflake オブジェクトを整理した全体像です。

---

## 1. プロジェクトルート構成

```
SF_FINANCE_DEMO/
├── .cursorrules              # Cursor ルール
├── README.md                 # プロジェクト説明
├── docs/                     # ドキュメント（本ファイル等）
│   └── ENVIRONMENT_OVERVIEW.md
├── dbt_finance_demo/         # dbt プロジェクト本体
├── SQL_FINANCE_DEMO_DB/      # Snowflake 用生 SQL（DDL/DML/管理）
├── work/                     # 作業用・検証用 SQL
├── logs/                     # ログ（dbt.log 等）
└── venv/                     # Python 仮想環境（dbt 実行用）
```

---

## 2. Snowflake 側の構成（SQL_FINANCE_DEMO_DB で管理）

### 2.1 作成スクリプトの配置

| ディレクトリ | 内容 |
|-------------|------|
| `01_01_CREATE_FINANCE_DEMO_DB/` | DB・WH・ロール・スキーマの作成 |
| `01_02_DDL_FINANCE_DEMO_DB/` | RAW / SEMANTIC スキーマの DDL（テーブル定義） |
| `01_03_DML_FINANCE_DEMO_DB/` | RAW への投入、STAGING/CORE/SEMANTIC の参照・更新用 DML |
| `02_01_DBT_MANAGE/` | dbt 運用メモ |
| `03_semantic_view_model/` | Snowflake Semantic Model / Cortex 用 YAML |
| `04_SBI_TEST/` | SBI 系テスト用（Raw Vault / Business Vault / Staging） |

### 2.2 データベース・ウェアハウス・ロール

| 種別 | 名前 | 説明 |
|------|------|------|
| Database | `FINANCE_DEMO_DB` | 金融デモ用データベース |
| Warehouse | `FINANCE_DEMO_WH` | 標準 WH（XSMALL, AUTO_SUSPEND=60, リソースモニターあり） |
| Role | `FINANCE_ADMIN_ROLE` | dbt 実行・オブジェクト管理用ロール |

### 2.3 スキーマ一覧

| スキーマ | 用途 | 主な作成元 |
|----------|------|------------|
| `RAW` | ソースデータ（注文・約定・預り・顧客・マスタ等） | 01_02 DDL + 01_03 DML |
| `STAGING` | RAW の正規化・ハッシュキー付与（View） | dbt staging モデル |
| `CORE` | Data Vault / ディメンション・ファクト（Table） | dbt core モデル |
| `SEMANTIC` | 分析・レポート用集計（Table / Stage） | dbt semantic モデル + 04_create_semantic.sql |
| `SBI_STAGING` | SBI テスト用 Staging | 04_SBI_TEST |
| `SBI_RAW_VAULT` | SBI テスト用 Raw Vault（DDL/DML） | 04_SBI_TEST |
| `SBI_BUSINESS_VAULT` | SBI テスト用 Business Vault | 04_SBI_TEST |
| `SBI_SEMANTIC` | SBI テスト用 Semantic | 04_SBI_TEST |
| `INTEGRATIONS` | API 統合用 | 04_01_create_schema.sql |

### 2.4 RAW スキーマのテーブル（ソース）

| 分類 | テーブル名 | 説明 |
|------|------------|------|
| 共通 | `CUSTOMER_PROFILES` | 顧客・口座属性 |
| 共通 | `STOCK_MASTER` | 銘柄マスタ（国内・外国） |
| 国内現物 | `STOCK_CASH_ORDERS` | 現物注文明細 |
| 国内現物 | `STOCK_CASH_EXECUTIONS` | 現物約定明細 |
| 国内現物 | `STOCK_CASH_HOLDINGS` | 現物預り明細（残高） |
| 国内信用 | `STOCK_MARGIN_ORDERS` | 信用注文明細 |
| 国内信用 | `STOCK_MARGIN_EXECUTIONS` | 信用約定明細 |
| 国内信用 | `STOCK_MARGIN_HOLDINGS` | 信用建玉明細 |
| 外国株 | `FOREIGN_STOCK_ORDERS` | 外国株注文明細 |
| 外国株 | `FOREIGN_STOCK_EXECUTIONS` | 外国株約定明細 |
| 外国株 | `FOREIGN_STOCK_HOLDINGS` | 外国株預り明細 |
| 外国株信用 | `FOREIGN_MARGIN_EXECUTIONS` | 外国株信用約定 |
| 外国株信用 | `FOREIGN_MARGIN_HOLDINGS` | 外国株信用建玉 |
| 資金・市場 | `CASH_TRANSACTIONS` | 入出金明細 |
| 資金・市場 | `EXCHANGE_RATES` | 為替レート |
| 資金・市場 | `STOCK_PRICES` | 日次時価（終値等） |

---

## 3. dbt プロジェクト（dbt_finance_demo）

### 3.1 dbt_project.yml の要点

| 項目 | 値 |
|------|-----|
| プロジェクト名 | `dbt_finance_demo` |
| バージョン | `1.0.0` |
| プロファイル | `dbt_finance_demo` |
| config-version | 2 |

**パス設定**

| キー | パス |
|------|------|
| model-paths | `models` |
| analysis-paths | `analyses` |
| test-paths | `tests` |
| seed-paths | `seeds` |
| macro-paths | `macros` |
| snapshot-paths | `snapshots` |

**モデルレイヤー別設定**

| フォルダ | スキーマ | マテリアライズ | persist_docs |
|----------|----------|----------------|--------------|
| `staging` | `STAGING` | view | relation / columns: true |
| `core` | `CORE` | table | relation / columns: true |
| `semantic` | `SEMANTIC` | table | relation / columns: true |

※ ルートの `models/` 直下や `utilities/` は上記フォルダ以外のため、デフォルトの target スキーマ等が使われる。

### 3.2 profiles.yml（接続情報）

| 項目 | 値 |
|------|-----|
| target | `STAGING`（デフォルト） |
| type | snowflake |
| account | Flux-SAND-GLOBAL |
| authenticator | externalbrowser（SSO） |
| role | FINANCE_ADMIN_ROLE |
| warehouse | FINANCE_DEMO_WH |
| database | FINANCE_DEMO_DB |
| schema | STAGING（target に連動） |
| threads | 4 |

### 3.3 パッケージ（packages.yml）

| パッケージ | バージョン | 用途 |
|------------|------------|------|
| dbt-labs/dbt_utils | 1.1.1 | ユーティリティマクロ |
| calogica/dbt_expectations | 0.10.1 | データ品質テスト（型・一意等） |

---

## 4. dbt モデル一覧

### 4.1 Staging（STAGING スキーマ / View）

RAW を `source('finance_raw', 'テーブル名')` で参照し、ハッシュキー（`*_hk`）・HashDiff・`base_date`・`load_date`・`record_source` を付与。

| モデル名 | 対応 RAW テーブル |
|----------|-------------------|
| stg_brand_master | stock_master |
| stg_customer_profiles | customer_profiles |
| stg_stock_cash_orders | stock_cash_orders |
| stg_stock_cash_executions | stock_cash_executions |
| stg_stock_cash_holdings | stock_cash_holdings |
| stg_stock_margin_orders | stock_margin_orders |
| stg_stock_margin_executions | stock_margin_executions |
| stg_stock_margin_holdings | stock_margin_holdings |
| stg_foreign_stock_orders | foreign_stock_orders |
| stg_foreign_stock_executions | foreign_stock_executions |
| stg_foreign_stock_holdings | foreign_stock_holdings |
| stg_foreign_margin_executions | foreign_margin_executions |
| stg_foreign_margin_holdings | foreign_margin_holdings |
| stg_cash_transactions | cash_transactions |

### 4.2 Core（CORE スキーマ / Table）

Data Vault 系（Hub / Link / Satellite / PIT）と分析用（dim / fact）。一部は Dynamic Table。

| 種別 | モデル名 | 備考 |
|------|----------|------|
| Hub | hub_account, hub_brand, hub_execution, hub_order | |
| Link | link_account_brand_holding, link_account_transaction, link_order_account_brand, link_order_execution | |
| Satellite | sat_brand_details, sat_customer_details, sat_execution_details, sat_stock_cash_holdings_details, sat_stock_margin_holdings_details | |
| PIT | pit_account | 口座時点テーブル |
| Dim/Fact | dim_account, fact_daily_holdings | 一部 dynamic_table |

### 4.3 Semantic（SEMANTIC スキーマ / Table）

| モデル名 | 説明 |
|----------|------|
| report_domestic_stocks | 国内株レポート（稼働口座数・新規稼働・継続稼働・現物残高保有・新規開設数等） |
| base_analysis | 分析用ベーステンプレート（Staging 参照の集計サンプル） |

### 4.4 その他

| 場所 | モデル | 備考 |
|------|--------|------|
| models/ 直下 | my_first_dbt_model, my_second_dbt_model | サンプル |
| utilities/ | all_days | 日付ユーティリティ |

---

## 5. ソース定義（src_finance.yml）

- **ソース名**: `finance_raw`
- **database / schema**: `FINANCE_DEMO_DB` / `RAW`
- **テーブル**: 上記 RAW テーブル一覧と対応。主要キー（order_id, execution_id, account_id, brand_cd 等）に unique / not_null / relationships のテストを定義。

---

## 6. テスト（tests/）

| ファイル | 内容 |
|----------|------|
| assert_positive_prices.sql | 現物・信用の約定で `price <= 0` が存在すると失敗（singular test） |
| assert_sat_unique_load_date.sql | 同一 order_hk・load_date で複数 order_hashdiff が存在すると失敗（singular test） |

---

## 7. マクロ（macros/）

| ファイル | 内容 |
|----------|------|
| get_custom_schema.sql | `generate_schema_name`: カスタムスキーマ指定時はそのまま使用し、未指定時は target.schema を使用。 |

---

## 8. データフロー概要

```
[RAW] ソーステーブル（DDL/DML で作成・投入）
        ↓ source('finance_raw', ...)
[STAGING] dbt staging モデル（View）
        ↓ ref('stg_*')
[CORE] Hub / Link / Satellite / PIT / dim / fact（Table）
        ↓ ref('pit_account'), ref('sat_*'), ref('dim_*'), ref('fact_*')
[SEMANTIC] レポート・分析用（Table）
```

---

## 9. work/ ディレクトリ（作業用）

検証・開発用の SQL が格納。本番 dbt モデルには含めない想定。

- mart_*.sql, fct_*.sql, sat_*_details.sql, pit_*.sql, vw_*.sql 等

---

## 10. 参照・更新の目安

| やりたいこと | 参照する場所 |
|--------------|--------------|
| Snowflake の DB/WH/ロール/スキーマ作成 | SQL_FINANCE_DEMO_DB/01_01_CREATE_FINANCE_DEMO_DB/ |
| RAW テーブル定義 | SQL_FINANCE_DEMO_DB/01_02_DDL_FINANCE_DEMO_DB/01_create_raw.sql |
| RAW 投入・Staging/Core/Semantic 参照 | SQL_FINANCE_DEMO_DB/01_03_DML_FINANCE_DEMO_DB/ |
| dbt のモデル・スキーマ・マテリアライズ | dbt_finance_demo/dbt_project.yml |
| dbt 接続・target | dbt_finance_demo/profiles.yml |
| ソーステーブル一覧・テスト定義 | dbt_finance_demo/models/staging/src_finance.yml |
| Staging モデル追加 | dbt_finance_demo/models/staging/ |
| Core モデル追加 | dbt_finance_demo/models/core/ + schema.yaml |
| 分析・レポート用モデル追加 | dbt_finance_demo/models/semantic/ |
| データ品質テスト追加 | dbt_finance_demo/tests/ |

---

*最終更新: プロジェクト構成に基づく整理*
