-- Oracle Test Case Script for gt-checksum

-- Create user and grant privileges (must be done before setting schema)
BEGIN
    EXECUTE IMMEDIATE 'CREATE USER gt_checksum IDENTIFIED BY gt_checksum';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE TRIGGER, CREATE PROCEDURE, CREATE FUNCTION TO gt_checksum';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'ALTER USER gt_checksum QUOTA UNLIMITED ON USERS';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

-- Connect as gt_checksum user
ALTER SESSION SET CURRENT_SCHEMA = gt_checksum;

-- Drop existing objects
BEGIN
    EXECUTE IMMEDIATE 'DROP TRIGGER tri_test';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP FUNCTION getAgeStr';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE test1';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE test2';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE indext';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tb_emp6';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tb_dept1';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE customer';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE customer1';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE range_hash_partition_table';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE hash_partition_table';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE list_partition_table';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE range_partition_table';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tmp_account';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE account';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testbin';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE teststring';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testtime';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testbit';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testfloat';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testint';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE t1';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE t2';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/



-- Test basic data types
CREATE TABLE testint(
    f1 NUMBER(3),
    f2 NUMBER(5),
    f3 NUMBER(8),
    f4 NUMBER(10),
    f5 NUMBER(5),
    f6 NUMBER(10),
    f7 NUMBER(19)
);
CREATE INDEX idx_testint_1 ON testint(f1);
INSERT INTO testint(f1,f2,f3,f4,f5,f6,f7) VALUES(1,2,3,4,5,6,7);

CREATE TABLE testfloat(
    f1 FLOAT,
    f2 FLOAT(5),
    f3 BINARY_DOUBLE,
    f4 BINARY_DOUBLE
);
CREATE INDEX idx_testfloat_1 ON testfloat(f1);
INSERT INTO testfloat(f1,f2,f3,f4) VALUES(123.45,123.45,123.45,12.456);

CREATE TABLE testbit(
    f1 NUMBER(1),
    f2 NUMBER(5),
    f3 NUMBER(19)
);
CREATE INDEX idx_testbit_1 ON testbit(f1);
INSERT INTO testbit VALUES(1,31,65);

CREATE TABLE testtime(
    f1 NUMBER(4),
    f2 NUMBER(4),
    f3 DATE,
    f4 INTERVAL DAY TO SECOND,
    f5 TIMESTAMP,
    f6 TIMESTAMP
);
CREATE INDEX idx_testtime_1 ON testtime(f1);
INSERT INTO testtime (f1, f2, f3, f4, f5, f6) VALUES (2022,2022,TO_DATE('2022-07-12', 'YYYY-MM-DD'),TO_DSINTERVAL('0 12:30:29'),TO_TIMESTAMP('2022-07-12 14:53:00', 'YYYY-MM-DD HH24:MI:SS'),TO_TIMESTAMP('2022-07-12 14:54:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO testtime (f1, f2, f3, f4, f5, f6) VALUES (2026,2026,TO_DATE('2026-02-12', 'YYYY-MM-DD'),TO_DSINTERVAL('0 15:15:30'),TO_TIMESTAMP('2026-02-12 14:53:00', 'YYYY-MM-DD HH24:MI:SS'),TO_TIMESTAMP('2026-02-12 14:54:00', 'YYYY-MM-DD HH24:MI:SS'));

CREATE TABLE teststring(
    f1 CHAR(1),
    f2 CHAR(5),
    f3 VARCHAR2(10),
    f4 VARCHAR2(4000),
    f5 CLOB,
    f6 CLOB,
    f7 CLOB,
    f8 VARCHAR2(1),
    f9 VARCHAR2(50)
);
CREATE INDEX idx_teststring_1 ON teststring(f1);
INSERT INTO teststring(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('1','abcde','abc123','abcd.1234','hello gt-checksum','hello ','hello gt-checksum','a','aa,bb');
INSERT INTO teststring(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('2','fghij','def456','efgh.5678','hello, i''m gt-checksum','hello ','hello gt-checksum','b','cc,dd');
INSERT INTO teststring(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('3','klmno','ghi789','ijkl.9012','a\b''c','hello ','hello gt-checksum','c','cc,dd');

CREATE TABLE testbin(
    f1 RAW(1),
    f2 RAW(3),
    f3 RAW(10),
    f4 BLOB, -- => CLOB
    f5 BLOB,
    f6 BLOB,
    f7 BLOB
);
CREATE INDEX idx_testbin_1 ON testbin(f1);
INSERT INTO testbin(f1,f2,f3,f4,f5,f6,f7) 
VALUES(
    HEXTORAW('61'), 
    HEXTORAW('616263'), 
    HEXTORAW('6162642e31323334'),
    HEXTORAW('01010101'),
    UTL_RAW.CAST_TO_RAW('9023123123'),
    UTL_RAW.CAST_TO_RAW('hello gt-checksum'),
    UTL_RAW.CAST_TO_RAW('hello gt-checksum')
);

-- Test triggers with account table
CREATE TABLE account (
    acct_num NUMBER(10), 
    amount NUMBER(10,2)
);
INSERT INTO account VALUES(137,14.98);
INSERT INTO account VALUES(141,1937.50);
INSERT INTO account VALUES(97,-100.00);

-- Create shadow table
CREATE TABLE tmp_account (
    acct_num NUMBER(10), 
    amount NUMBER(10,2),
    sql_text VARCHAR2(100)
);

-- Create trigger for INSERT
CREATE OR REPLACE TRIGGER accountInsert 
BEFORE INSERT ON account 
FOR EACH ROW
BEGIN
    INSERT INTO tmp_account(acct_num,amount,sql_text) VALUES(:NEW.acct_num,:NEW.amount,'INSERT');
END;
/

-- Create trigger for DELETE  
CREATE OR REPLACE TRIGGER accountDelete
BEFORE DELETE ON account 
FOR EACH ROW
BEGIN
    INSERT INTO tmp_account(acct_num,amount,sql_text) VALUES(:OLD.acct_num,:OLD.amount,'DELETE');
END;
/

-- Create trigger for UPDATE
CREATE OR REPLACE TRIGGER accountUpdate
BEFORE UPDATE ON account 
FOR EACH ROW
BEGIN
    INSERT INTO tmp_account(acct_num,amount,sql_text) VALUES(:OLD.acct_num,:OLD.amount,'UPDATE_DELETE');
    INSERT INTO tmp_account(acct_num,amount,sql_text) VALUES(:NEW.acct_num,:NEW.amount,'UPDATE_INSERT');
END;
/

-- Test trigger functionality
INSERT INTO account VALUES (150,33.32);

INSERT INTO account VALUES(200,13.23);
UPDATE account SET acct_num = 201 WHERE amount = 13.23;

INSERT INTO account VALUES(300,14.23);
DELETE FROM account WHERE acct_num = 300;

-- Test partition tables
CREATE TABLE range_partition_table(
    range_key_column DATE,
    name VARCHAR2(20),
    id NUMBER
) PARTITION BY RANGE(range_key_column)(
    PARTITION PART_202007 VALUES LESS THAN (TO_DATE('2020-07-01','YYYY-MM-DD')),
    PARTITION PART_202008 VALUES LESS THAN (TO_DATE('2020-08-01','YYYY-MM-DD')),
    PARTITION PART_202009 VALUES LESS THAN (TO_DATE('2020-09-01','YYYY-MM-DD'))
);

CREATE TABLE customer(
    customer_id NUMBER NOT NULL PRIMARY KEY,
    first_name  VARCHAR2(30) NOT NULL,
    last_name   VARCHAR2(30) NOT NULL,
    phone       VARCHAR2(15) NOT NULL,
    email       VARCHAR2(80),
    status      CHAR(1)
) PARTITION BY RANGE (customer_id)(
    PARTITION CUS_PART1 VALUES LESS THAN (100000),
    PARTITION CUS_PART2 VALUES LESS THAN (200000)
);

CREATE TABLE customer1(
    customer_id NUMBER NOT NULL,
    first_name  VARCHAR2(30) NOT NULL,
    last_name   VARCHAR2(30) NOT NULL,
    phone       VARCHAR2(15) NOT NULL,
    email       VARCHAR2(80),
    status      CHAR(1)
) PARTITION BY RANGE (customer_id)(
    PARTITION CUS_PART1 VALUES LESS THAN (100000),
    PARTITION CUS_PART2 VALUES LESS THAN (200000)
);

CREATE TABLE list_partition_table(
    name VARCHAR2(10),
    data VARCHAR2(20)
) PARTITION BY LIST(name)(
    PARTITION PART_01 VALUES ('ME','PE','QC','RD'),
    PARTITION PART_02 VALUES ('SMT','SALE')
);

CREATE TABLE hash_partition_table(
    hash_key_column NUMBER(30),
    data VARCHAR2(20)
) PARTITION BY HASH(hash_key_column)
PARTITIONS 4;

CREATE TABLE range_hash_partition_table (
    id NUMBER,
    purchased DATE,
    data VARCHAR2(20),
    purchase_year NUMBER,
    purchase_day_of_year VARCHAR2(3)
)
PARTITION BY RANGE(purchase_year)
    SUBPARTITION BY HASH(purchase_day_of_year)
    SUBPARTITIONS 2 (
        PARTITION p0 VALUES LESS THAN (1990),
        PARTITION p1 VALUES LESS THAN (2000),
        PARTITION p2 VALUES LESS THAN (MAXVALUE)
);

-- Test foreign key constraints
CREATE TABLE tb_dept1 (
    id NUMBER(11) PRIMARY KEY,
    name VARCHAR2(22) NOT NULL,
    location VARCHAR2(50)
);

CREATE TABLE tb_emp6(
    id NUMBER(11) PRIMARY KEY,
    name VARCHAR2(25),
    deptid NUMBER(11),
    salary FLOAT,
    CONSTRAINT fk_emp_dept1
    FOREIGN KEY(deptid) REFERENCES tb_dept1(id)
);

-- Test stored procedures and functions
CREATE OR REPLACE FUNCTION getAgeStr(age IN NUMBER)
RETURN VARCHAR2 IS
    results VARCHAR2(20);
BEGIN
    IF age <= 14 THEN
        results := 'Children';
    ELSIF age <= 24 THEN
        results := 'Teenagers';
    ELSIF age <= 44 THEN
        results := 'Youth';
    ELSIF age <= 59 THEN
        results := 'Middle Age';
    ELSE
        results := 'Elderly';
    END IF;
    RETURN results;
END getAgeStr;
/

-- Test additional triggers
CREATE TABLE test1(
    a1 NUMBER
);
CREATE TABLE test2(
    a2 NUMBER
);

CREATE OR REPLACE TRIGGER tri_test
BEFORE INSERT ON test1
FOR EACH ROW
BEGIN
    INSERT INTO test2(a2) VALUES(:NEW.a1);
END;
/

-- Test indexes with complex structure
CREATE TABLE indext(
    id NUMBER(11) NOT NULL,
    tenantry_id NUMBER(20) NOT NULL,
    code VARCHAR2(64) NOT NULL,
    goods_name VARCHAR2(50) NOT NULL,
    props_name VARCHAR2(100) NOT NULL,
    price NUMBER(10,2) NOT NULL,
    price_url VARCHAR2(1000) NOT NULL,
    create_time TIMESTAMP NOT NULL,
    modify_time TIMESTAMP DEFAULT NULL,
    deleted NUMBER(1) default 0 NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX idx_indext_2 ON indext(tenantry_id,code);
CREATE INDEX idx_indext_3 ON indext(code,tenantry_id);
INSERT INTO indext VALUES ('583532949','8674665223082153551','aut','animi','eum','1.99','fugit',TO_TIMESTAMP('2026-02-17 16:04:25', 'YYYY-MM-DD HH24:MI:SS'),TO_TIMESTAMP('2025-06-20 22:10:41', 'YYYY-MM-DD HH24:MI:SS'),'1');
INSERT INTO indext VALUES ('914246705','2020683354385918016','quam','aut','cumque','0.00','nihil',TO_TIMESTAMP('2025-03-20 01:01:33', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2025-07-27 22:10:28', 'YYYY-MM-DD HH24:MI:SS'),'2');

-- 测试从Oracle=>MySQL数据同步
CREATE TABLE t1 (
    id NUMBER(19) NOT NULL,
    c_varchar2 VARCHAR2(4000),
    c_char CHAR(10),
    c_nchar NCHAR(10),
    c_nvarchar2 NVARCHAR2(1000),
    c_number NUMBER(38,5),
    c_float FLOAT(126),
    c_decimal DECIMAL(10,2),
    c_date DATE,
    c_timestamp TIMESTAMP(6),
    c_clob CLOB,
    c_boolean NUMBER(1), 
    PRIMARY KEY (id)
);

-- 1. 常规标准数据
INSERT INTO t1 VALUES (
    1,
    'Standard English Text',
    'A',
    N'NCHAR值',
    N'NVARCHAR2标准文本',
    12345.6789,
    123.456,
    99.99,
    TO_DATE('2023-10-01 12:30:00', 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP('2023-10-01 12:30:00.123456', 'YYYY-MM-DD HH24:MI:SS.FF6'),
    TO_CLOB('Standard CLOB text data.'),
    1
);

-- 2. 边界值与特殊字符 (包含Emoji、极大极小值、年份极值)
INSERT INTO t1 VALUES (
    2,
    'Special chars: ~!@#$%^&*()_+{}|:"<>? / 汉字 / 🚀',
    'CHAR10    ',
    N'测试    ',
    N'多语言: こんにちは, 안녕하세요, 🚀',
    999999999999999999999999999999999.99999,
    9007199254740991,
    -99999999.99,
    TO_DATE('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP('1970-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'),
    TO_CLOB(RPAD('A', 10, 'A')) || RPAD('B', 10, 'B'),
    0
);

-- 3. 全 NULL 值测试 (主键除外)
INSERT INTO t1 (id) VALUES (3);

-- 补充测试：完整覆盖 Oracle=>MySQL 常用数据类型映射
-- 重点补齐 t1 未覆盖的 NUMBER(p,0)=>INT/BIGINT/SMALLINT、BLOB=>LONGBLOB、RAW=>VARBINARY
CREATE TABLE t2 (
    id          NUMBER(10) NOT NULL,   -- NUMBER(p,0) => INT
    c_bigint    NUMBER(19),             -- NUMBER(p,0) => BIGINT
    c_smallint  NUMBER(5),              -- NUMBER(p,0) => SMALLINT
    c_varchar2  VARCHAR2(255),          -- VARCHAR2(n) => VARCHAR(n)
    c_char      CHAR(5),                -- CHAR(n)     => CHAR(n)
    c_nchar     NCHAR(5),               -- NCHAR(n)    => CHAR(n) utf8mb4
    c_nvarchar2 NVARCHAR2(100),         -- NVARCHAR2(n)=> VARCHAR(n) utf8mb4
    c_number    NUMBER(10,2),           -- NUMBER(p,s) => DECIMAL(p,s)
    c_float     FLOAT,                  -- FLOAT       => DOUBLE
    c_decimal   DECIMAL(8,3),           -- DECIMAL     => DECIMAL
    c_date      DATE,                   -- DATE        => DATETIME
    c_timestamp TIMESTAMP(3),           -- TIMESTAMP(n)=> DATETIME(n)
    c_clob      CLOB,                   -- CLOB        => LONGTEXT
    c_blob      BLOB,                   -- BLOB        => LONGBLOB
    c_raw       RAW(16),                -- RAW(n)      => VARBINARY(n)
    c_bool      NUMBER(1),              -- NUMBER(1)   => TINYINT(1)
    PRIMARY KEY (id)
);

-- 1. 常规标准数据
INSERT INTO t2 VALUES (
    1,
    9223372036854775807,
    32767,
    'Standard VARCHAR2 text',
    'ABCDE',
    N'中文5',
    N'多语言 NVARCHAR2',
    12345.67,
    123.456,
    99.125,
    TO_DATE('2023-10-01 12:30:00', 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP('2023-10-01 12:30:00.123', 'YYYY-MM-DD HH24:MI:SS.FF3'),
    TO_CLOB('Standard CLOB data.'),
    HEXTORAW('48656C6C6F20474343'),
    HEXTORAW('DEADBEEFCAFEBABE'),
    1
);

-- 2. 边界值与特殊字符
INSERT INTO t2 VALUES (
    2,
    -9223372036854775808,
    -32768,
    'Special: ~!@#$%^&*()_+ 汉字 / 🚀',
    'X    ',
    N'Z    ',
    N'こんにちは, 안녕하세요, 🚀',
    -99999999.99,
    9007199254740991,
    -99999.999,
    TO_DATE('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP('1970-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),
    TO_CLOB('Special CLOB: 汉字/🚀'),
    UTL_RAW.CAST_TO_RAW('hello blob'),
    HEXTORAW('00FF00FF00FF00FF'),
    0
);

-- 3. 全 NULL 值测试 (主键除外)
INSERT INTO t2 (id) VALUES (3);

COMMIT;
