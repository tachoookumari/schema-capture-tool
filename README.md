# schema-capture-tool

PostgreSQL および Oracle のスキーマ情報を取得・格納するツールです。  
データベース移行プロジェクトにおけるスキーマ比較・検証用途を想定しています。

---

## 概要

データベースのスキーマ情報（テーブル・カラム・インデックス・制約・ビュー・シーケンス・トリガー・プロシージャ）を一括取得し、管理テーブルへ格納します。  
取得日時付きで履歴管理ができるため、移行前後の比較や差分確認に活用できます。

---

## ファイル構成

| ファイル名 | 説明 |
|---|---|
| `schema_info_postgresql.sql` | PostgreSQL用 スキーマ情報取得ツール |
| `schema_info_oracle.sql` | Oracle用 スキーマ情報取得ツール |

---

## 取得できる情報

| 種別 | 格納テーブル | 主な取得内容 |
|---|---|---|
| テーブル情報 | `schema_tables` | テーブル名・行数・最終分析日時 等 |
| カラム情報 | `schema_columns` | カラム名・データ型・NULL可否・デフォルト値 等 |
| インデックス情報 | `schema_indexes` | インデックス名・種別・ユニーク有無・カラムリスト 等 |
| 制約情報 | `schema_constraints` | 制約名・制約種別・カラムリスト 等 |
| ビュー情報 | `schema_views` | ビュー名・定義SQL 等 |
| シーケンス情報 | `schema_sequences` | シーケンス名・最小/最大値・増分値 等 |
| トリガー情報 | `schema_triggers` | トリガー名・イベント・本体SQL 等 |
| プロシージャ情報 | `schema_procedures` | プロシージャ/ファンクション名・ソースコード 等 |

---

## 使い方（PostgreSQL）

### 1. テーブル・プロシージャの作成

```sql
\i schema_info_postgresql.sql
```

### 2. スキーマ情報の取得

```sql
CALL capture_schema_info('your_schema_name');
```

### 3. 取得結果の確認

```sql
-- テーブル一覧
SELECT table_name, num_rows, last_analyzed
FROM schema_tables
WHERE capture_date = (SELECT MAX(capture_date) FROM schema_tables)
  AND schema_name = 'your_schema_name'
ORDER BY table_name;

-- カラム情報
SELECT column_name, data_type, nullable
FROM schema_columns
WHERE capture_date = (SELECT MAX(capture_date) FROM schema_columns)
  AND schema_name = 'your_schema_name'
  AND table_name = 'your_table_name'
ORDER BY column_id;
```

---

## 動作確認済み環境

- PostgreSQL 10以降
- Oracle 11g以降

---

## 活用シーン

- Oracle → PostgreSQL 移行時のスキーマ比較
- 移行前後のテーブル構造・カラム定義の差分確認
- DB定期メンテナンス時のスキーマ変更履歴管理

---

## 履歴データの削除

```sql
-- 30日より古いデータを削除
DELETE FROM schema_tables      WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_columns     WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_indexes     WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_constraints WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_views       WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_sequences   WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_triggers    WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_procedures  WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
```
