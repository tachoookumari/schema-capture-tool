-- ============================================================
-- Oracle スキーマ情報取得・格納システム
-- ============================================================
-- ============================================================
-- 1. 格納テーブルの作成
-- ============================================================
-- テーブル情報
CREATE TABLE SCHEMA_TABLES (
    CAPTURE_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
    SCHEMA_NAME VARCHAR2(128),
    TABLE_NAME VARCHAR2(128),
    TABLESPACE_NAME VARCHAR2(128),
    NUM_ROWS NUMBER,
    BLOCKS NUMBER,
    AVG_ROW_LEN NUMBER,
    LAST_ANALYZED DATE,
    PARTITIONED VARCHAR2(3),
    TEMPORARY VARCHAR2(1),
    CONSTRAINT PK_SCHEMA_TABLES PRIMARY KEY (CAPTURE_DATE, SCHEMA_NAME, TABLE_NAME)
);
-- カラム情報
CREATE TABLE SCHEMA_COLUMNS (
    CAPTURE_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
    SCHEMA_NAME VARCHAR2(128),
    TABLE_NAME VARCHAR2(128),
    COLUMN_NAME VARCHAR2(128),
    COLUMN_ID NUMBER,
    DATA_TYPE VARCHAR2(128),
    DATA_LENGTH NUMBER,
    DATA_PRECISION NUMBER,
    DATA_SCALE NUMBER,
    NULLABLE VARCHAR2(1),
    DATA_DEFAULT LONG,
    CONSTRAINT PK_SCHEMA_COLUMNS PRIMARY KEY (
        CAPTURE_DATE,
        SCHEMA_NAME,
        TABLE_NAME,
        COLUMN_NAME
    )
);
-- インデックス情報
CREATE TABLE SCHEMA_INDEXES (
    CAPTURE_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
    SCHEMA_NAME VARCHAR2(128),
    INDEX_NAME VARCHAR2(128),
    TABLE_NAME VARCHAR2(128),
    INDEX_TYPE VARCHAR2(27),
    UNIQUENESS VARCHAR2(9),
    TABLESPACE_NAME VARCHAR2(128),
    COLUMN_LIST VARCHAR2(4000),
    CONSTRAINT PK_SCHEMA_INDEXES PRIMARY KEY (CAPTURE_DATE, SCHEMA_NAME, INDEX_NAME)
);
-- 制約情報
CREATE TABLE SCHEMA_CONSTRAINTS (
    CAPTURE_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
    SCHEMA_NAME VARCHAR2(128),
    CONSTRAINT_NAME VARCHAR2(128),
    CONSTRAINT_TYPE VARCHAR2(1),
    TABLE_NAME VARCHAR2(128),
    SEARCH_CONDITION LONG,
    R_OWNER VARCHAR2(128),
    R_CONSTRAINT_NAME VARCHAR2(128),
    DELETE_RULE VARCHAR2(9),
    STATUS VARCHAR2(8),
    VALIDATED VARCHAR2(13),
    COLUMN_LIST VARCHAR2(4000),
    CONSTRAINT PK_SCHEMA_CONSTRAINTS PRIMARY KEY (CAPTURE_DATE, SCHEMA_NAME, CONSTRAINT_NAME)
);
-- ビュー情報
CREATE TABLE SCHEMA_VIEWS (
    CAPTURE_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
    SCHEMA_NAME VARCHAR2(128),
    VIEW_NAME VARCHAR2(128),
    TEXT_LENGTH NUMBER,
    TEXT CLOB,
    TYPE_TEXT VARCHAR2(4000),
    CONSTRAINT PK_SCHEMA_VIEWS PRIMARY KEY (CAPTURE_DATE, SCHEMA_NAME, VIEW_NAME)
);
-- シーケンス情報
CREATE TABLE SCHEMA_SEQUENCES (
    CAPTURE_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
    SCHEMA_NAME VARCHAR2(128),
    SEQUENCE_NAME VARCHAR2(128),
    MIN_VALUE NUMBER,
    MAX_VALUE NUMBER,
    INCREMENT_BY NUMBER,
    CYCLE_FLAG VARCHAR2(1),
    ORDER_FLAG VARCHAR2(1),
    CACHE_SIZE NUMBER,
    LAST_NUMBER NUMBER,
    CONSTRAINT PK_SCHEMA_SEQUENCES PRIMARY KEY (CAPTURE_DATE, SCHEMA_NAME, SEQUENCE_NAME)
);
-- トリガー情報
CREATE TABLE SCHEMA_TRIGGERS (
    CAPTURE_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
    SCHEMA_NAME VARCHAR2(128),
    TRIGGER_NAME VARCHAR2(128),
    TRIGGER_TYPE VARCHAR2(16),
    TRIGGERING_EVENT VARCHAR2(227),
    TABLE_NAME VARCHAR2(128),
    STATUS VARCHAR2(8),
    TRIGGER_BODY CLOB,
    CONSTRAINT PK_SCHEMA_TRIGGERS PRIMARY KEY (CAPTURE_DATE, SCHEMA_NAME, TRIGGER_NAME)
);
-- プロシージャ/ファンクション情報
CREATE TABLE SCHEMA_PROCEDURES (
    CAPTURE_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
    SCHEMA_NAME VARCHAR2(128),
    OBJECT_NAME VARCHAR2(128),
    OBJECT_TYPE VARCHAR2(23),
    STATUS VARCHAR2(7),
    CREATED DATE,
    LAST_DDL_TIME DATE,
    SOURCE_CODE CLOB,
    CONSTRAINT PK_SCHEMA_PROCEDURES PRIMARY KEY (
        CAPTURE_DATE,
        SCHEMA_NAME,
        OBJECT_NAME,
        OBJECT_TYPE
    )
);
-- ============================================================
-- 2. スキーマ情報取得プロシージャ
-- ============================================================
CREATE OR REPLACE PROCEDURE CAPTURE_SCHEMA_INFO (p_schema_name IN VARCHAR2) AS v_capture_date TIMESTAMP := SYSTIMESTAMP;
v_column_list VARCHAR2(4000);
BEGIN -- ============================================================
-- テーブル情報の取得
-- ============================================================
INSERT INTO SCHEMA_TABLES (
        CAPTURE_DATE,
        SCHEMA_NAME,
        TABLE_NAME,
        TABLESPACE_NAME,
        NUM_ROWS,
        BLOCKS,
        AVG_ROW_LEN,
        LAST_ANALYZED,
        PARTITIONED,
        TEMPORARY
    )
SELECT v_capture_date,
    OWNER,
    TABLE_NAME,
    TABLESPACE_NAME,
    NUM_ROWS,
    BLOCKS,
    AVG_ROW_LEN,
    LAST_ANALYZED,
    PARTITIONED,
    TEMPORARY
FROM DBA_TABLES
WHERE OWNER = UPPER(p_schema_name)
    AND TABLE_NAME NOT LIKE 'BIN$%';
-- ゴミ箱のテーブルを除外
DBMS_OUTPUT.PUT_LINE('テーブル情報: ' || SQL %ROWCOUNT || '件取得');
-- ============================================================
-- カラム情報の取得
-- ============================================================
INSERT INTO SCHEMA_COLUMNS (
        CAPTURE_DATE,
        SCHEMA_NAME,
        TABLE_NAME,
        COLUMN_NAME,
        COLUMN_ID,
        DATA_TYPE,
        DATA_LENGTH,
        DATA_PRECISION,
        DATA_SCALE,
        NULLABLE,
        DATA_DEFAULT
    )
SELECT v_capture_date,
    OWNER,
    TABLE_NAME,
    COLUMN_NAME,
    COLUMN_ID,
    DATA_TYPE,
    DATA_LENGTH,
    DATA_PRECISION,
    DATA_SCALE,
    NULLABLE,
    DATA_DEFAULT
FROM DBA_TAB_COLUMNS
WHERE OWNER = UPPER(p_schema_name)
    AND TABLE_NAME NOT LIKE 'BIN$%'
ORDER BY TABLE_NAME,
    COLUMN_ID;
DBMS_OUTPUT.PUT_LINE('カラム情報: ' || SQL %ROWCOUNT || '件取得');
-- ============================================================
-- インデックス情報の取得
-- ============================================================
FOR idx_rec IN (
    SELECT OWNER,
        INDEX_NAME,
        TABLE_NAME,
        INDEX_TYPE,
        UNIQUENESS,
        TABLESPACE_NAME
    FROM DBA_INDEXES
    WHERE OWNER = UPPER(p_schema_name)
        AND TABLE_NAME NOT LIKE 'BIN$%'
) LOOP -- カラムリストの取得
SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (
        ORDER BY COLUMN_POSITION
    ) INTO v_column_list
FROM DBA_IND_COLUMNS
WHERE INDEX_OWNER = idx_rec.OWNER
    AND INDEX_NAME = idx_rec.INDEX_NAME;
INSERT INTO SCHEMA_INDEXES (
        CAPTURE_DATE,
        SCHEMA_NAME,
        INDEX_NAME,
        TABLE_NAME,
        INDEX_TYPE,
        UNIQUENESS,
        TABLESPACE_NAME,
        COLUMN_LIST
    )
VALUES (
        v_capture_date,
        idx_rec.OWNER,
        idx_rec.INDEX_NAME,
        idx_rec.TABLE_NAME,
        idx_rec.INDEX_TYPE,
        idx_rec.UNIQUENESS,
        idx_rec.TABLESPACE_NAME,
        v_column_list
    );
END LOOP;
DBMS_OUTPUT.PUT_LINE('インデックス情報: ' || SQL %ROWCOUNT || '件取得');
-- ============================================================
-- 制約情報の取得
-- ============================================================
FOR cons_rec IN (
    SELECT OWNER,
        CONSTRAINT_NAME,
        CONSTRAINT_TYPE,
        TABLE_NAME,
        SEARCH_CONDITION,
        R_OWNER,
        R_CONSTRAINT_NAME,
        DELETE_RULE,
        STATUS,
        VALIDATED
    FROM DBA_CONSTRAINTS
    WHERE OWNER = UPPER(p_schema_name)
        AND TABLE_NAME NOT LIKE 'BIN$%'
) LOOP -- カラムリストの取得
BEGIN
SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (
        ORDER BY POSITION
    ) INTO v_column_list
FROM DBA_CONS_COLUMNS
WHERE OWNER = cons_rec.OWNER
    AND CONSTRAINT_NAME = cons_rec.CONSTRAINT_NAME;
EXCEPTION
WHEN NO_DATA_FOUND THEN v_column_list := NULL;
END;
INSERT INTO SCHEMA_CONSTRAINTS (
        CAPTURE_DATE,
        SCHEMA_NAME,
        CONSTRAINT_NAME,
        CONSTRAINT_TYPE,
        TABLE_NAME,
        SEARCH_CONDITION,
        R_OWNER,
        R_CONSTRAINT_NAME,
        DELETE_RULE,
        STATUS,
        VALIDATED,
        COLUMN_LIST
    )
VALUES (
        v_capture_date,
        cons_rec.OWNER,
        cons_rec.CONSTRAINT_NAME,
        cons_rec.CONSTRAINT_TYPE,
        cons_rec.TABLE_NAME,
        cons_rec.SEARCH_CONDITION,
        cons_rec.R_OWNER,
        cons_rec.R_CONSTRAINT_NAME,
        cons_rec.DELETE_RULE,
        cons_rec.STATUS,
        cons_rec.VALIDATED,
        v_column_list
    );
END LOOP;
DBMS_OUTPUT.PUT_LINE('制約情報: ' || SQL %ROWCOUNT || '件取得');
-- ============================================================
-- ビュー情報の取得
-- ============================================================
FOR view_rec IN (
    SELECT OWNER,
        VIEW_NAME,
        TEXT_LENGTH,
        TEXT,
        TYPE_TEXT
    FROM DBA_VIEWS
    WHERE OWNER = UPPER(p_schema_name)
) LOOP
INSERT INTO SCHEMA_VIEWS (
        CAPTURE_DATE,
        SCHEMA_NAME,
        VIEW_NAME,
        TEXT_LENGTH,
        TEXT,
        TYPE_TEXT
    )
VALUES (
        v_capture_date,
        view_rec.OWNER,
        view_rec.VIEW_NAME,
        view_rec.TEXT_LENGTH,
        view_rec.TEXT,
        view_rec.TYPE_TEXT
    );
END LOOP;
DBMS_OUTPUT.PUT_LINE('ビュー情報: ' || SQL %ROWCOUNT || '件取得');
-- ============================================================
-- シーケンス情報の取得
-- ============================================================
INSERT INTO SCHEMA_SEQUENCES (
        CAPTURE_DATE,
        SCHEMA_NAME,
        SEQUENCE_NAME,
        MIN_VALUE,
        MAX_VALUE,
        INCREMENT_BY,
        CYCLE_FLAG,
        ORDER_FLAG,
        CACHE_SIZE,
        LAST_NUMBER
    )
SELECT v_capture_date,
    SEQUENCE_OWNER,
    SEQUENCE_NAME,
    MIN_VALUE,
    MAX_VALUE,
    INCREMENT_BY,
    CYCLE_FLAG,
    ORDER_FLAG,
    CACHE_SIZE,
    LAST_NUMBER
FROM DBA_SEQUENCES
WHERE SEQUENCE_OWNER = UPPER(p_schema_name);
DBMS_OUTPUT.PUT_LINE('シーケンス情報: ' || SQL %ROWCOUNT || '件取得');
-- ============================================================
-- トリガー情報の取得
-- ============================================================
FOR trig_rec IN (
    SELECT OWNER,
        TRIGGER_NAME,
        TRIGGER_TYPE,
        TRIGGERING_EVENT,
        TABLE_NAME,
        STATUS,
        TRIGGER_BODY
    FROM DBA_TRIGGERS
    WHERE OWNER = UPPER(p_schema_name)
) LOOP
INSERT INTO SCHEMA_TRIGGERS (
        CAPTURE_DATE,
        SCHEMA_NAME,
        TRIGGER_NAME,
        TRIGGER_TYPE,
        TRIGGERING_EVENT,
        TABLE_NAME,
        STATUS,
        TRIGGER_BODY
    )
VALUES (
        v_capture_date,
        trig_rec.OWNER,
        trig_rec.TRIGGER_NAME,
        trig_rec.TRIGGER_TYPE,
        trig_rec.TRIGGERING_EVENT,
        trig_rec.TABLE_NAME,
        trig_rec.STATUS,
        trig_rec.TRIGGER_BODY
    );
END LOOP;
DBMS_OUTPUT.PUT_LINE('トリガー情報: ' || SQL %ROWCOUNT || '件取得');
-- ============================================================
-- プロシージャ/ファンクション情報の取得
-- ============================================================
FOR proc_rec IN (
    SELECT OWNER,
        OBJECT_NAME,
        OBJECT_TYPE,
        STATUS,
        CREATED,
        LAST_DDL_TIME
    FROM DBA_OBJECTS
    WHERE OWNER = UPPER(p_schema_name)
        AND OBJECT_TYPE IN (
            'PROCEDURE',
            'FUNCTION',
            'PACKAGE',
            'PACKAGE BODY',
            'TYPE',
            'TYPE BODY'
        )
) LOOP -- ソースコードの結合
DECLARE v_source CLOB;
BEGIN FOR src IN (
    SELECT TEXT
    FROM DBA_SOURCE
    WHERE OWNER = proc_rec.OWNER
        AND NAME = proc_rec.OBJECT_NAME
        AND TYPE = proc_rec.OBJECT_TYPE
    ORDER BY LINE
) LOOP v_source := v_source || src.TEXT;
END LOOP;
INSERT INTO SCHEMA_PROCEDURES (
        CAPTURE_DATE,
        SCHEMA_NAME,
        OBJECT_NAME,
        OBJECT_TYPE,
        STATUS,
        CREATED,
        LAST_DDL_TIME,
        SOURCE_CODE
    )
VALUES (
        v_capture_date,
        proc_rec.OWNER,
        proc_rec.OBJECT_NAME,
        proc_rec.OBJECT_TYPE,
        proc_rec.STATUS,
        proc_rec.CREATED,
        proc_rec.LAST_DDL_TIME,
        v_source
    );
END;
END LOOP;
DBMS_OUTPUT.PUT_LINE('プロシージャ/ファンクション情報: ' || SQL %ROWCOUNT || '件取得');
COMMIT;
DBMS_OUTPUT.PUT_LINE('===================================');
DBMS_OUTPUT.PUT_LINE('スキーマ情報の取得が完了しました');
DBMS_OUTPUT.PUT_LINE('対象スキーマ: ' || p_schema_name);
DBMS_OUTPUT.PUT_LINE(
    '取得日時: ' || TO_CHAR(v_capture_date, 'YYYY-MM-DD HH24:MI:SS')
);
DBMS_OUTPUT.PUT_LINE('===================================');
EXCEPTION
WHEN OTHERS THEN ROLLBACK;
DBMS_OUTPUT.PUT_LINE('エラーが発生しました: ' || SQLERRM);
RAISE;
END CAPTURE_SCHEMA_INFO;
/ -- ============================================================
-- 3. 使用例とサンプルクエリ
-- ============================================================
/*
 -- プロシージャの実行例
 SET SERVEROUTPUT ON
 EXEC CAPTURE_SCHEMA_INFO('YOUR_SCHEMA_NAME');
 
 -- 最新のテーブル一覧を確認
 SELECT TABLE_NAME, NUM_ROWS, LAST_ANALYZED
 FROM SCHEMA_TABLES
 WHERE CAPTURE_DATE = (SELECT MAX(CAPTURE_DATE) FROM SCHEMA_TABLES)
 AND SCHEMA_NAME = 'YOUR_SCHEMA_NAME'
 ORDER BY TABLE_NAME;
 
 -- 特定テーブルのカラム情報を確認
 SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
 FROM SCHEMA_COLUMNS
 WHERE CAPTURE_DATE = (SELECT MAX(CAPTURE_DATE) FROM SCHEMA_COLUMNS)
 AND SCHEMA_NAME = 'YOUR_SCHEMA_NAME'
 AND TABLE_NAME = 'YOUR_TABLE_NAME'
 ORDER BY COLUMN_ID;
 
 -- インデックスの一覧
 SELECT INDEX_NAME, TABLE_NAME, UNIQUENESS, COLUMN_LIST
 FROM SCHEMA_INDEXES
 WHERE CAPTURE_DATE = (SELECT MAX(CAPTURE_DATE) FROM SCHEMA_INDEXES)
 AND SCHEMA_NAME = 'YOUR_SCHEMA_NAME'
 ORDER BY TABLE_NAME, INDEX_NAME;
 
 -- 制約の一覧
 SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE, TABLE_NAME, COLUMN_LIST
 FROM SCHEMA_CONSTRAINTS
 WHERE CAPTURE_DATE = (SELECT MAX(CAPTURE_DATE) FROM SCHEMA_CONSTRAINTS)
 AND SCHEMA_NAME = 'YOUR_SCHEMA_NAME'
 ORDER BY TABLE_NAME, CONSTRAINT_TYPE;
 
 -- 履歴の確認（テーブル数の推移）
 SELECT 
 TO_CHAR(CAPTURE_DATE, 'YYYY-MM-DD HH24:MI:SS') AS 取得日時,
 COUNT(*) AS テーブル数
 FROM SCHEMA_TABLES
 WHERE SCHEMA_NAME = 'YOUR_SCHEMA_NAME'
 GROUP BY CAPTURE_DATE
 ORDER BY CAPTURE_DATE DESC;
 
 -- 古い履歴の削除（30日より古いデータを削除する例）
 DELETE FROM SCHEMA_TABLES WHERE CAPTURE_DATE < SYSTIMESTAMP - 30;
 DELETE FROM SCHEMA_COLUMNS WHERE CAPTURE_DATE < SYSTIMESTAMP - 30;
 DELETE FROM SCHEMA_INDEXES WHERE CAPTURE_DATE < SYSTIMESTAMP - 30;
 DELETE FROM SCHEMA_CONSTRAINTS WHERE CAPTURE_DATE < SYSTIMESTAMP - 30;
 DELETE FROM SCHEMA_VIEWS WHERE CAPTURE_DATE < SYSTIMESTAMP - 30;
 DELETE FROM SCHEMA_SEQUENCES WHERE CAPTURE_DATE < SYSTIMESTAMP - 30;
 DELETE FROM SCHEMA_TRIGGERS WHERE CAPTURE_DATE < SYSTIMESTAMP - 30;
 DELETE FROM SCHEMA_PROCEDURES WHERE CAPTURE_DATE < SYSTIMESTAMP - 30;
 COMMIT;
 */