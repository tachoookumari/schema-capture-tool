-- ============================================================
-- PostgreSQL スキーマ情報取得・格納システム
-- Oracle版からの変換
-- ============================================================

-- ============================================================
-- 1. 格納テーブルの作成
-- ============================================================

-- テーブル情報
CREATE TABLE schema_tables (
    capture_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name      VARCHAR(128),
    table_name       VARCHAR(128),
    tablespace_name  VARCHAR(128),
    num_rows         BIGINT,
    blocks           BIGINT,
    avg_row_len      BIGINT,
    last_analyzed    TIMESTAMP,
    partitioned      VARCHAR(3),
    temporary        VARCHAR(1),
    CONSTRAINT pk_schema_tables PRIMARY KEY (capture_date, schema_name, table_name)
);

-- カラム情報
CREATE TABLE schema_columns (
    capture_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name     VARCHAR(128),
    table_name      VARCHAR(128),
    column_name     VARCHAR(128),
    column_id       INTEGER,
    data_type       VARCHAR(128),
    data_length     INTEGER,
    data_precision  INTEGER,
    data_scale      INTEGER,
    nullable        VARCHAR(1),
    data_default    TEXT,
    CONSTRAINT pk_schema_columns PRIMARY KEY (capture_date, schema_name, table_name, column_name)
);

-- インデックス情報
CREATE TABLE schema_indexes (
    capture_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name      VARCHAR(128),
    index_name       VARCHAR(128),
    table_name       VARCHAR(128),
    index_type       VARCHAR(27),
    uniqueness       VARCHAR(9),
    tablespace_name  VARCHAR(128),
    column_list      TEXT,
    CONSTRAINT pk_schema_indexes PRIMARY KEY (capture_date, schema_name, index_name)
);

-- 制約情報
CREATE TABLE schema_constraints (
    capture_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name        VARCHAR(128),
    constraint_name    VARCHAR(128),
    constraint_type    VARCHAR(1),
    table_name         VARCHAR(128),
    search_condition   TEXT,
    r_owner            VARCHAR(128),
    r_constraint_name  VARCHAR(128),
    delete_rule        VARCHAR(9),
    status             VARCHAR(8),
    validated          VARCHAR(13),
    column_list        TEXT,
    CONSTRAINT pk_schema_constraints PRIMARY KEY (capture_date, schema_name, constraint_name)
);

-- ビュー情報
CREATE TABLE schema_views (
    capture_date  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name   VARCHAR(128),
    view_name     VARCHAR(128),
    text_length   INTEGER,
    text          TEXT,
    type_text     VARCHAR(4000),
    CONSTRAINT pk_schema_views PRIMARY KEY (capture_date, schema_name, view_name)
);

-- シーケンス情報
CREATE TABLE schema_sequences (
    capture_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name    VARCHAR(128),
    sequence_name  VARCHAR(128),
    min_value      BIGINT,
    max_value      BIGINT,
    increment_by   BIGINT,
    cycle_flag     VARCHAR(1),
    order_flag     VARCHAR(1),
    cache_size     BIGINT,
    last_number    BIGINT,
    CONSTRAINT pk_schema_sequences PRIMARY KEY (capture_date, schema_name, sequence_name)
);

-- トリガー情報
CREATE TABLE schema_triggers (
    capture_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name       VARCHAR(128),
    trigger_name      VARCHAR(128),
    trigger_type      VARCHAR(16),
    triggering_event  VARCHAR(227),
    table_name        VARCHAR(128),
    status            VARCHAR(8),
    trigger_body      TEXT,
    CONSTRAINT pk_schema_triggers PRIMARY KEY (capture_date, schema_name, trigger_name)
);

-- プロシージャ/ファンクション情報
CREATE TABLE schema_procedures (
    capture_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name    VARCHAR(128),
    object_name    VARCHAR(128),
    object_type    VARCHAR(23),
    status         VARCHAR(7),
    created        TIMESTAMP,
    last_ddl_time  TIMESTAMP,
    source_code    TEXT,
    CONSTRAINT pk_schema_procedures PRIMARY KEY (capture_date, schema_name, object_name, object_type)
);


-- ============================================================
-- 2. スキーマ情報取得プロシージャ
-- ============================================================

CREATE OR REPLACE PROCEDURE capture_schema_info(
    p_schema_name IN VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_capture_date  TIMESTAMP := CURRENT_TIMESTAMP;
    v_column_list   TEXT;
    v_count         INTEGER;

    -- インデックス用カーソル
    idx_rec RECORD;
    -- 制約用カーソル
    cons_rec RECORD;
    -- ビュー用カーソル
    view_rec RECORD;
    -- トリガー用カーソル
    trig_rec RECORD;
    -- プロシージャ用カーソル
    proc_rec RECORD;
BEGIN

    -- ============================================================
    -- テーブル情報の取得
    -- ※ PostgreSQLにはOracle互換の統計情報ビューはないため
    --   pg_stat_user_tables / pg_class を使用
    -- ============================================================
    INSERT INTO schema_tables (
        capture_date, schema_name, table_name, tablespace_name,
        num_rows, blocks, avg_row_len, last_analyzed,
        partitioned, temporary
    )
    SELECT
        v_capture_date,
        n.nspname,                          -- schema_name
        c.relname,                          -- table_name
        ts.spcname,                         -- tablespace_name
        c.reltuples::BIGINT,                -- num_rows (推定値)
        c.relpages,                         -- blocks
        CASE WHEN c.reltuples > 0
             THEN (pg_relation_size(c.oid) / c.reltuples)::BIGINT
             ELSE 0
        END,                                -- avg_row_len
        s.last_analyze,                     -- last_analyzed
        CASE WHEN c.relkind = 'p' THEN 'YES' ELSE 'NO' END,  -- partitioned
        CASE WHEN c.relpersistence = 't' THEN 'Y' ELSE 'N' END  -- temporary
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
    LEFT JOIN pg_stat_user_tables s
           ON s.schemaname = n.nspname AND s.relname = c.relname
    WHERE n.nspname = LOWER(p_schema_name)
      AND c.relkind IN ('r', 'p')   -- 通常テーブル・パーティションテーブル
      AND c.relname NOT LIKE 'pg_%';

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'テーブル情報: %件取得', v_count;

    -- ============================================================
    -- カラム情報の取得
    -- ============================================================
    INSERT INTO schema_columns (
        capture_date, schema_name, table_name, column_name,
        column_id, data_type, data_length, data_precision,
        data_scale, nullable, data_default
    )
    SELECT
        v_capture_date,
        table_schema,
        table_name,
        column_name,
        ordinal_position,
        udt_name,                           -- PostgreSQL型名
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        CASE WHEN is_nullable = 'YES' THEN 'Y' ELSE 'N' END,
        column_default
    FROM information_schema.columns
    WHERE table_schema = LOWER(p_schema_name)
    ORDER BY table_name, ordinal_position;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'カラム情報: %件取得', v_count;

    -- ============================================================
    -- インデックス情報の取得
    -- ============================================================
    v_count := 0;
    FOR idx_rec IN (
        SELECT
            n.nspname                                   AS schema_name,
            i.relname                                   AS index_name,
            t.relname                                   AS table_name,
            am.amname                                   AS index_type,
            CASE WHEN ix.indisunique THEN 'UNIQUE' ELSE 'NONUNIQUE' END AS uniqueness,
            ts.spcname                                  AS tablespace_name,
            ix.indkey,
            t.oid                                       AS table_oid
        FROM pg_index ix
        JOIN pg_class i  ON i.oid  = ix.indexrelid
        JOIN pg_class t  ON t.oid  = ix.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am    ON am.oid  = i.relam
        LEFT JOIN pg_tablespace ts ON ts.oid = i.reltablespace
        WHERE n.nspname = LOWER(p_schema_name)
          AND t.relkind = 'r'
    ) LOOP
        -- カラムリストの取得
        SELECT string_agg(a.attname, ', ' ORDER BY array_position(ix2.indkey, a.attnum))
        INTO v_column_list
        FROM pg_index ix2
        JOIN pg_attribute a ON a.attrelid = idx_rec.table_oid
                           AND a.attnum = ANY(ix2.indkey)
        WHERE ix2.indexrelid = (
            SELECT i2.oid FROM pg_class i2
            JOIN pg_namespace n2 ON n2.oid = i2.relnamespace
            WHERE n2.nspname = idx_rec.schema_name
              AND i2.relname = idx_rec.index_name
        );

        INSERT INTO schema_indexes (
            capture_date, schema_name, index_name, table_name,
            index_type, uniqueness, tablespace_name, column_list
        ) VALUES (
            v_capture_date,
            idx_rec.schema_name,
            idx_rec.index_name,
            idx_rec.table_name,
            idx_rec.index_type,
            idx_rec.uniqueness,
            idx_rec.tablespace_name,
            v_column_list
        );
        v_count := v_count + 1;
    END LOOP;

    RAISE NOTICE 'インデックス情報: %件取得', v_count;

    -- ============================================================
    -- 制約情報の取得
    -- ============================================================
    v_count := 0;
    FOR cons_rec IN (
        SELECT
            n.nspname                       AS schema_name,
            c.conname                       AS constraint_name,
            CASE c.contype
                WHEN 'p' THEN 'P'
                WHEN 'u' THEN 'U'
                WHEN 'f' THEN 'R'
                WHEN 'c' THEN 'C'
                ELSE c.contype::TEXT
            END                             AS constraint_type,
            t.relname                       AS table_name,
            pg_get_constraintdef(c.oid)     AS search_condition,
            rn.nspname                      AS r_owner,
            rc.conname                      AS r_constraint_name,
            CASE c.confdeltype
                WHEN 'a' THEN 'NO ACTION'
                WHEN 'r' THEN 'RESTRICT'
                WHEN 'c' THEN 'CASCADE'
                WHEN 'n' THEN 'SET NULL'
                WHEN 'd' THEN 'SET DEFAULT'
                ELSE NULL
            END                             AS delete_rule,
            'ENABLED'                       AS status,
            'VALIDATED'                     AS validated,
            c.oid                           AS con_oid,
            t.oid                           AS table_oid
        FROM pg_constraint c
        JOIN pg_class t       ON t.oid = c.conrelid
        JOIN pg_namespace n   ON n.oid = t.relnamespace
        LEFT JOIN pg_constraint rc ON rc.oid = c.confrelid  -- 参照先制約(FKの場合)
        LEFT JOIN pg_namespace rn  ON rn.oid = (
            SELECT relnamespace FROM pg_class WHERE oid = c.confrelid
        )
        WHERE n.nspname = LOWER(p_schema_name)
    ) LOOP
        -- カラムリストの取得
        SELECT string_agg(a.attname, ', ' ORDER BY kcv.pos)
        INTO v_column_list
        FROM (
            SELECT unnest(conkey) AS attnum, generate_subscripts(conkey, 1) AS pos
            FROM pg_constraint WHERE oid = cons_rec.con_oid
        ) kcv
        JOIN pg_attribute a ON a.attrelid = cons_rec.table_oid
                           AND a.attnum = kcv.attnum;

        INSERT INTO schema_constraints (
            capture_date, schema_name, constraint_name, constraint_type,
            table_name, search_condition, r_owner, r_constraint_name,
            delete_rule, status, validated, column_list
        ) VALUES (
            v_capture_date,
            cons_rec.schema_name,
            cons_rec.constraint_name,
            cons_rec.constraint_type,
            cons_rec.table_name,
            cons_rec.search_condition,
            cons_rec.r_owner,
            cons_rec.r_constraint_name,
            cons_rec.delete_rule,
            cons_rec.status,
            cons_rec.validated,
            v_column_list
        );
        v_count := v_count + 1;
    END LOOP;

    RAISE NOTICE '制約情報: %件取得', v_count;

    -- ============================================================
    -- ビュー情報の取得
    -- ============================================================
    v_count := 0;
    FOR view_rec IN (
        SELECT
            table_schema  AS schema_name,
            table_name    AS view_name,
            view_definition AS text
        FROM information_schema.views
        WHERE table_schema = LOWER(p_schema_name)
    ) LOOP
        INSERT INTO schema_views (
            capture_date, schema_name, view_name,
            text_length, text, type_text
        ) VALUES (
            v_capture_date,
            view_rec.schema_name,
            view_rec.view_name,
            length(view_rec.text),
            view_rec.text,
            NULL  -- PostgreSQLにはOracle互換のTYPE_TEXTなし
        );
        v_count := v_count + 1;
    END LOOP;

    RAISE NOTICE 'ビュー情報: %件取得', v_count;

    -- ============================================================
    -- シーケンス情報の取得
    -- ============================================================
    INSERT INTO schema_sequences (
        capture_date, schema_name, sequence_name,
        min_value, max_value, increment_by,
        cycle_flag, order_flag, cache_size, last_number
    )
    SELECT
        v_capture_date,
        sequence_schema,
        sequence_name,
        minimum_value::BIGINT,
        maximum_value::BIGINT,
        increment::BIGINT,
        CASE WHEN cycle_option = 'YES' THEN 'Y' ELSE 'N' END,
        'N',   -- PostgreSQLにはORDERオプションなし
        cache_size::BIGINT,
        -- last_valueはpg_sequencesから取得
        (SELECT last_value FROM pg_sequences
          WHERE schemaname = sequence_schema
            AND sequencename = sequence_name)
    FROM information_schema.sequences
    WHERE sequence_schema = LOWER(p_schema_name);

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'シーケンス情報: %件取得', v_count;

    -- ============================================================
    -- トリガー情報の取得
    -- ============================================================
    v_count := 0;
    FOR trig_rec IN (
        SELECT
            trigger_schema     AS schema_name,
            trigger_name,
            action_timing || ' ' || action_orientation AS trigger_type,
            event_manipulation AS triggering_event,
            event_object_table AS table_name,
            'ENABLED'          AS status,
            action_statement   AS trigger_body
        FROM information_schema.triggers
        WHERE trigger_schema = LOWER(p_schema_name)
    ) LOOP
        INSERT INTO schema_triggers (
            capture_date, schema_name, trigger_name,
            trigger_type, triggering_event, table_name,
            status, trigger_body
        ) VALUES (
            v_capture_date,
            trig_rec.schema_name,
            trig_rec.trigger_name,
            trig_rec.trigger_type,
            trig_rec.triggering_event,
            trig_rec.table_name,
            trig_rec.status,
            trig_rec.trigger_body
        );
        v_count := v_count + 1;
    END LOOP;

    RAISE NOTICE 'トリガー情報: %件取得', v_count;

    -- ============================================================
    -- プロシージャ/ファンクション情報の取得
    -- ============================================================
    v_count := 0;
    FOR proc_rec IN (
        SELECT
            n.nspname                   AS schema_name,
            p.proname                   AS object_name,
            CASE p.prokind
                WHEN 'f' THEN 'FUNCTION'
                WHEN 'p' THEN 'PROCEDURE'
                WHEN 'a' THEN 'AGGREGATE'
                WHEN 'w' THEN 'WINDOW'
                ELSE 'FUNCTION'
            END                         AS object_type,
            'VALID'                     AS status,
            NULL::TIMESTAMP             AS created,
            NULL::TIMESTAMP             AS last_ddl_time,
            pg_get_functiondef(p.oid)   AS source_code
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = LOWER(p_schema_name)
          AND p.prokind IN ('f', 'p')   -- ファンクションとプロシージャのみ
    ) LOOP
        INSERT INTO schema_procedures (
            capture_date, schema_name, object_name,
            object_type, status, created,
            last_ddl_time, source_code
        ) VALUES (
            v_capture_date,
            proc_rec.schema_name,
            proc_rec.object_name,
            proc_rec.object_type,
            proc_rec.status,
            proc_rec.created,
            proc_rec.last_ddl_time,
            proc_rec.source_code
        );
        v_count := v_count + 1;
    END LOOP;

    RAISE NOTICE 'プロシージャ/ファンクション情報: %件取得', v_count;

    RAISE NOTICE '===================================';
    RAISE NOTICE 'スキーマ情報の取得が完了しました';
    RAISE NOTICE '対象スキーマ: %', p_schema_name;
    RAISE NOTICE '取得日時: %', TO_CHAR(v_capture_date, 'YYYY-MM-DD HH24:MI:SS');
    RAISE NOTICE '===================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'エラーが発生しました: %', SQLERRM;
END;
$$;


-- ============================================================
-- 3. 使用例とサンプルクエリ
-- ============================================================

/*
-- プロシージャの実行例
CALL capture_schema_info('your_schema_name');

-- 最新のテーブル一覧を確認
SELECT table_name, num_rows, last_analyzed
FROM schema_tables
WHERE capture_date = (SELECT MAX(capture_date) FROM schema_tables)
  AND schema_name = 'your_schema_name'
ORDER BY table_name;

-- 特定テーブルのカラム情報を確認
SELECT column_name, data_type, nullable
FROM schema_columns
WHERE capture_date = (SELECT MAX(capture_date) FROM schema_columns)
  AND schema_name = 'your_schema_name'
  AND table_name = 'your_table_name'
ORDER BY column_id;

-- インデックスの一覧
SELECT index_name, table_name, uniqueness, column_list
FROM schema_indexes
WHERE capture_date = (SELECT MAX(capture_date) FROM schema_indexes)
  AND schema_name = 'your_schema_name'
ORDER BY table_name, index_name;

-- 制約の一覧
SELECT constraint_name, constraint_type, table_name, column_list
FROM schema_constraints
WHERE capture_date = (SELECT MAX(capture_date) FROM schema_constraints)
  AND schema_name = 'your_schema_name'
ORDER BY table_name, constraint_type;

-- 履歴の確認（テーブル数の推移）
SELECT
    TO_CHAR(capture_date, 'YYYY-MM-DD HH24:MI:SS') AS 取得日時,
    COUNT(*) AS テーブル数
FROM schema_tables
WHERE schema_name = 'your_schema_name'
GROUP BY capture_date
ORDER BY capture_date DESC;

-- 古い履歴の削除（30日より古いデータを削除する例）
DELETE FROM schema_tables      WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_columns     WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_indexes     WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_constraints WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_views       WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_sequences   WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_triggers    WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
DELETE FROM schema_procedures  WHERE capture_date < CURRENT_TIMESTAMP - INTERVAL '30 days';
*/
