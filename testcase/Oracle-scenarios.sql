-- =============================================================================
-- gt-checksum Oracle→MySQL 场景回归 fixture（Oracle 源端）
-- 适用于: scripts/regression-test-oracle.sh --with-scenarios
-- 对应：docs/large-file-v3-dev-plan-cc-20260422.md Step 0-C 4 条 + 清单 ⑥⑦
-- 用户 gt_checksum 已在 testcase/Oracle.sql 初始化中创建，本 fixture 仅处理表
-- =============================================================================
ALTER SESSION SET CURRENT_SCHEMA = gt_checksum;

-- 统一的 "DROP TABLE IF EXISTS" 包装
BEGIN EXECUTE IMMEDIATE 'DROP TABLE sc_types'     ; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "ScCase"'     ; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE sc_part'      ; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE sc_largeobj'  ; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE sc_missing'   ; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE sc_incompat'  ; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- ----------------------------------------------------------------------------
-- TC-ORAST-01: 列类型映射 NUMBER/VARCHAR2/DATE/TIMESTAMP → DECIMAL/VARCHAR/DATETIME
-- ----------------------------------------------------------------------------
CREATE TABLE sc_types (
    id          NUMBER(10)    NOT NULL PRIMARY KEY,
    amount      NUMBER(12,2)  DEFAULT 0,
    name        VARCHAR2(100),
    born_date   DATE,
    updated_at  TIMESTAMP
);
INSERT INTO sc_types (id, amount, name, born_date, updated_at)
    VALUES (1, 100.50, 'alice', DATE '2020-01-01', SYSTIMESTAMP);
INSERT INTO sc_types (id, amount, name, born_date, updated_at)
    VALUES (2, 200.00, 'bob',   DATE '2021-06-15', SYSTIMESTAMP);
COMMIT;

-- ----------------------------------------------------------------------------
-- TC-ORAST-02: 大小写敏感标识符（引号保留原样大小写）
-- ----------------------------------------------------------------------------
CREATE TABLE "ScCase" (
    "Id"     NUMBER(10) NOT NULL PRIMARY KEY,
    "FullName" VARCHAR2(50)
);
INSERT INTO "ScCase" ("Id", "FullName") VALUES (1, 'Carol');
COMMIT;

-- ----------------------------------------------------------------------------
-- TC-ORAST-03: 分区表 RANGE 分区
-- ----------------------------------------------------------------------------
CREATE TABLE sc_part (
    id      NUMBER(10) NOT NULL,
    ymd     DATE       NOT NULL,
    val     VARCHAR2(32)
)
PARTITION BY RANGE (ymd) (
    PARTITION p2023 VALUES LESS THAN (DATE '2024-01-01'),
    PARTITION p2024 VALUES LESS THAN (DATE '2025-01-01'),
    PARTITION pmax  VALUES LESS THAN (MAXVALUE)
);
INSERT INTO sc_part VALUES (1, DATE '2023-06-01', 'row2023');
INSERT INTO sc_part VALUES (2, DATE '2024-06-01', 'row2024');
COMMIT;

-- ----------------------------------------------------------------------------
-- TC-ORAST-04: CLOB/BLOB/LONG RAW 大字段
-- ----------------------------------------------------------------------------
CREATE TABLE sc_largeobj (
    id     NUMBER(10) NOT NULL PRIMARY KEY,
    body   CLOB,
    photo  BLOB
);
INSERT INTO sc_largeobj VALUES (1, TO_CLOB('hello clob'), NULL);
COMMIT;

-- ----------------------------------------------------------------------------
-- TC-ORAST-05: MySQL 目标缺表 → struct 建表（对应清单⑥）
-- ----------------------------------------------------------------------------
CREATE TABLE sc_missing (
    id    NUMBER(10) NOT NULL PRIMARY KEY,
    val   VARCHAR2(50)
);

-- ----------------------------------------------------------------------------
-- TC-ORAST-06: data 模式硬不兼容（RAW 无直接默认映射）（对应清单⑦）
-- ----------------------------------------------------------------------------
CREATE TABLE sc_incompat (
    id     NUMBER(10) NOT NULL PRIMARY KEY,
    token  RAW(16)
);
INSERT INTO sc_incompat VALUES (1, HEXTORAW('0102030405060708090A0B0C0D0E0F10'));
COMMIT;
