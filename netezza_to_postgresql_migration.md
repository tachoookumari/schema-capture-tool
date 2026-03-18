# Netezza → PostgreSQL 移行手順書

**作成日**: 2026年3月  
**対象読者**: 中〜上級データベースエンジニア  
**対象バージョン**: Netezza（IBM PureData System for Analytics）→ PostgreSQL 14以降

---

## 1. 移行概要

### 1.1 背景・目的

IBM Netezza（PureData System for Analytics）はアプライアンス型のMPP（大規模並列処理）DWH製品であり、独自のSQL方言・関数・データ型を多数持つ。PostgreSQLへの移行にあたっては単純なデータ移送にとどまらず、SQLの書き換え・アーキテクチャの再設計が必要となる。

### 1.2 移行の全体フロー

```
[Phase 1] 現状調査・移行計画
        ↓
[Phase 2] スキーマ移行（DDL変換）
        ↓
[Phase 3] SQL・ロジック移行（DML/プロシージャ変換）
        ↓
[Phase 4] データ移行（ETL）
        ↓
[Phase 5] 検証・テスト
        ↓
[Phase 6] 本番切り替え（カットオーバー）
```

---

## 2. 使用ツール・アプリケーション

### 2.1 スキーマ・SQL変換ツール

| ツール | 用途 | 備考 |
|--------|------|------|
| **AWS Schema Conversion Tool (SCT)** | DDL・SQL自動変換 | AWSへの移行前提。変換率は高いが要手動確認 |
| **Ispirer MnMTK** | SQL変換（商用） | Netezza対応実績あり。精度高め |
| **pgloader** | データ＋スキーマ移行 | OSSで軽量。CSVやODBCソース対応 |
| **SQLines** | SQL構文変換（一部無料） | オンライン変換ツールも提供 |
| **自作スクリプト（Python/sed）** | 方言の一括置換 | 独自関数が多い場合に補完用途で使用 |

### 2.2 データ抽出・ロードツール

| ツール | 用途 | 備考 |
|--------|------|------|
| **nzsql / nz_backup** | Netezzaからのデータ抽出 | Netezza純正CLIツール |
| **COPY コマンド（PostgreSQL）** | CSVからの高速バルクロード | 最速のロード手段 |
| **pgloader** | Netezza ODBC経由で直接ロード | ODBCドライバ設定が必要 |
| **DataSpider / Talend / Informatica** | ETLツールによる変換・ロード | 既存ETL資産を活用する場合 |
| **AWS DMS（Database Migration Service）** | クラウド移行時のデータ移送 | AWSターゲット環境前提 |
| **Python（psycopg2 + pandas）** | カスタムETL実装 | 柔軟性高いが実装コスト大 |

### 2.3 検証ツール

| ツール | 用途 |
|--------|------|
| **pgTAP** | PostgreSQLユニットテストフレームワーク |
| **custom SQL（COUNT/CHECKSUM比較）** | レコード件数・チェックサム突合 |
| **dbt（data build tool）** | データ変換後の品質テスト |
| **psql / DBeaver / pgAdmin** | クエリ実行・目視確認 |

---

## 3. 移行手順（フェーズ別）

### Phase 1: 現状調査・移行計画

#### 1-1. Netezza環境の棚卸し

```sql
-- テーブル一覧・サイズ確認
SELECT tablename, objtype, owner
FROM _v_table
WHERE database = 'YOUR_DB'
ORDER BY tablename;

-- ディストリビューションキー確認
SELECT tablename, attname AS dist_key
FROM _v_table_dist_map
WHERE database = 'YOUR_DB';

-- ストアドプロシージャ一覧
SELECT procedure_name, procedure_returns, procedure_language
FROM _v_procedure
WHERE procedure_schema = 'YOUR_SCHEMA';
```

#### 1-2. 移行難易度の分類

| 分類 | 内容 | 対応方針 |
|------|------|----------|
| **A（自動変換可）** | 標準SQL、基本的なDDL | ツール変換後に確認のみ |
| **B（要手動修正）** | Netezza方言関数、DISTRIBUTE BY句など | 変換後に手動修正 |
| **C（要再設計）** | Netezza固有のゾーンマップ依存、SPU並列前提のSQL | アーキテクチャから見直し |

---

### Phase 2: スキーマ移行（DDL変換）

#### 2-1. 主要なデータ型変換対応表

| Netezza型 | PostgreSQL型 | 備考 |
|-----------|-------------|------|
| `BYTEINT` | `SMALLINT` | 1バイト整数→2バイトで代替 |
| `NUMERIC(p,s)` | `NUMERIC(p,s)` | ほぼ同一 |
| `NCHAR / NVARCHAR` | `CHAR / VARCHAR` | PostgreSQLはUTF-8ネイティブ |
| `ST_GEOMETRY` | `geometry`（PostGIS） | PostGIS拡張が必要 |
| `INTERVAL` | `INTERVAL` | 構文差異あり要確認 |

#### 2-2. DISTRIBUTE BY → 代替設計

Netezzaの `DISTRIBUTE BY` はPostgreSQLには存在しない。パフォーマンスへの影響を考慮した代替策：

```sql
-- Netezza
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    ...
) DISTRIBUTE ON (customer_id);

-- PostgreSQL：パーティショニング or インデックスで代替
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    ...
);
CREATE INDEX idx_orders_customer ON orders(customer_id);
```

大規模テーブルはパーティションテーブル（`PARTITION BY RANGE` / `LIST`）の採用を検討する。

---

### Phase 3: SQL・ロジック移行

#### 3-1. Netezza方言 → PostgreSQL変換早見表

| Netezza | PostgreSQL | 備考 |
|---------|-----------|------|
| `AGE_IN_YEARS(date)` | `DATE_PART('year', AGE(date))` | |
| `NOW()` | `NOW()` または `CURRENT_TIMESTAMP` | 同一 |
| `SUBSTR(str, pos, len)` | `SUBSTRING(str, pos, len)` | |
| `DECODE(col, val1, res1, ...)` | `CASE WHEN col = val1 THEN res1 ...` | |
| `TRANSLATE(str, from, to)` | `TRANSLATE(str, from, to)` | 同一 |
| `REGEXP_EXTRACT(str, pat)` | `REGEXP_MATCH(str, pat)[1]` | |
| `NULLIFZERO(col)` | `NULLIF(col, 0)` | |
| `ZEROIFNULL(col)` | `COALESCE(col, 0)` | |
| `||` 文字列結合 | `||` または `CONCAT()` | 同一 |
| `CAST(col AS FORMAT 'YYYY-MM-DD')` | `TO_CHAR(col, 'YYYY-MM-DD')` | |

#### 3-2. ストアドプロシージャ変換

NetezzaのSQLPL → PL/pgSQLへの変換が必要。

```sql
-- Netezza（SQLPL）
CREATE OR REPLACE PROCEDURE sp_sample (IN p_id INT)
LANGUAGE NZPLSQL
AS
BEGIN_PROC
    UPDATE orders SET status = 'done' WHERE order_id = p_id;
END_PROC;

-- PostgreSQL（PL/pgSQL）
CREATE OR REPLACE PROCEDURE sp_sample(p_id INT)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE orders SET status = 'done' WHERE order_id = p_id;
END;
$$;
```

---

### Phase 4: データ移行

#### 4-1. Netezzaからのデータ抽出

```bash
# nzsqlでCSVエクスポート
nzsql -d YOUR_DB -u admin -pw password \
  -c "\COPY (SELECT * FROM schema.tablename) TO '/tmp/tablename.csv' CSV HEADER"

# または nz_backupで論理バックアップ
nz_backup -db YOUR_DB -dir /backup/dir -t schema.tablename
```

#### 4-2. PostgreSQLへのデータロード

```sql
-- COPYコマンドによる高速バルクロード
\COPY schema.tablename FROM '/tmp/tablename.csv' 
  WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8');

-- psqlコマンドラインから
psql -U postgres -d target_db \
  -c "\COPY schema.tablename FROM '/tmp/tablename.csv' CSV HEADER"
```

#### 4-3. 大規模テーブルの分割ロード

```bash
# splitコマンドで分割してロード
split -l 1000000 tablename.csv chunk_
for f in chunk_*; do
  psql -U postgres -d target_db \
    -c "\COPY schema.tablename FROM '$f' CSV"
done
```

---

### Phase 5: 検証・テスト

#### 5-1. レコード件数突合

```sql
-- 移行元（Netezza）
SELECT COUNT(*) FROM schema.tablename;

-- 移行先（PostgreSQL）
SELECT COUNT(*) FROM schema.tablename;
```

#### 5-2. チェックサム突合（数値列）

```sql
-- PostgreSQL側
SELECT 
    COUNT(*) AS row_count,
    SUM(amount) AS sum_amount,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount
FROM schema.tablename;
```

#### 5-3. NULL分布・値分布確認

```sql
SELECT 
    column_name,
    COUNT(*) AS total,
    COUNT(column_name) AS non_null,
    COUNT(*) - COUNT(column_name) AS null_count
FROM schema.tablename
GROUP BY column_name;
```

---

### Phase 6: 本番切り替え（カットオーバー）

#### 6-1. カットオーバー手順（バッチ停止方式）

```
1. Netezza側のバッチ・ETL処理を停止
2. 差分データの最終抽出・ロード
3. PostgreSQL側の整合性確認（レコード数・チェックサム）
4. アプリケーション接続先をPostgreSQLに切り替え
5. 動作確認（スモークテスト）
6. 問題なければNetezzaをリードオンリーに変更
7. 一定期間後Netezza廃止
```

#### 6-2. ロールバック計画

| 状況 | 対応 |
|------|------|
| データ不整合発見 | 接続先をNetezzaに戻し原因調査 |
| パフォーマンス問題 | インデックス追加・クエリ最適化後に再切り替え |
| アプリケーションエラー | SQL方言の変換漏れを確認・修正 |

---

## 4. 注意点・ハマりどころ

### 4-1. NULL の扱いの差異

Netezzaは一部の関数でNULLを0として扱うケースがある。PostgreSQLは厳密にNULLを伝播させるため、集計結果が変わる可能性がある。

```sql
-- 要確認：SUM/AVGのNULL挙動
-- PostgreSQL: NULLがある行は集計から除外される（ANSI準拠）
SELECT AVG(amount) FROM orders; -- NULLは除外して平均計算
```

### 4-2. 文字コード・エンコーディング

NetezzaはUTF-8とLATIN-1混在の場合がある。PostgreSQL移行時に文字化けが発生しやすい。エクスポート時に明示的にエンコーディングを指定すること。

```bash
nzsql ... --encoding=UTF8
```

### 4-3. 大文字・小文字の扱い

Netezzaはデフォルトで識別子を**大文字に正規化**する。PostgreSQLは**小文字に正規化**する。ダブルクォートで囲まれた識別子は両者ともケースセンシティブ。移行後にSQL内の識別子の大文字小文字を統一すること。

### 4-4. DISTRIBUTE BY の性能影響

Netezzaの分散キーに相当するPostgreSQLの仕組みは存在しない。大規模テーブルのJOINパフォーマンスが劣化するケースがある。対策としてパーティショニング・インデックス設計の見直しを必須とする。

### 4-5. シーケンス・採番

Netezzaの `IDENTITY` 列はPostgreSQLの `SERIAL` または `GENERATED ALWAYS AS IDENTITY` に変換する。

```sql
-- Netezza
col INT GENERATED ALWAYS AS IDENTITY

-- PostgreSQL
col INT GENERATED ALWAYS AS IDENTITY
-- または
col SERIAL
```

### 4-6. CASE式とNULL

```sql
-- Netezzaではこれが期待通り動くケースでも
-- PostgreSQLではNULLの扱いに注意
CASE WHEN col = NULL THEN 'null_val' END
-- → PostgreSQLではIS NULLを使う
CASE WHEN col IS NULL THEN 'null_val' END
```

### 4-7. ストアドプロシージャの言語差異

NetezzaのSQLPL（NZPLSQL）はPL/pgSQLと構文が類似しているが、以下の点で差異がある：

- `BEGIN_PROC / END_PROC` → `BEGIN / END`
- `CALL` 文の構文差異
- 例外処理構文の差異（`WHEN OTHERS` → `WHEN OTHERS THEN`）
- `RAISEERROR` → `RAISE EXCEPTION`

### 4-8. 統計情報・VACUUM

PostgreSQLは `ANALYZE` による統計情報更新が必須。大量ロード後は必ず実行すること。

```sql
-- ロード後に必ず実行
ANALYZE schema.tablename;
VACUUM ANALYZE schema.tablename;
```

---

## 5. 移行チェックリスト

### スキーマ移行
- [ ] 全テーブルのDDL変換完了
- [ ] データ型変換の確認
- [ ] インデックス・制約の再作成
- [ ] シーケンスの移行
- [ ] ビューの移行・動作確認

### データ移行
- [ ] 全テーブルのレコード件数突合
- [ ] 数値集計値（SUM/AVG）の突合
- [ ] NULL分布の確認
- [ ] 文字コードの確認

### SQL・ロジック移行
- [ ] Netezza方言関数の置換完了
- [ ] ストアドプロシージャの動作確認
- [ ] バッチ・ETLジョブの動作確認

### パフォーマンス
- [ ] クエリ実行計画（EXPLAIN ANALYZE）の確認
- [ ] インデックス有効性の確認
- [ ] ANALYZE実行済み

### カットオーバー
- [ ] ロールバック手順の確認・合意
- [ ] 監視設定の確認
- [ ] 接続先切り替え確認

---

*本資料はNetezza → PostgreSQL移行における一般的な手順・注意点をまとめたものです。環境固有の要件については個別に調査・対応を行うこと。*
