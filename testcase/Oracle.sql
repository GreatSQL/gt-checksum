-- Oracle Test Case Script for gt-checksum
-- Reference: MySQL.sql adapted for Oracle syntax

-- Drop existing objects
BEGIN
    EXECUTE IMMEDIATE 'DROP TRIGGER tri_test';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP PROCEDURE countproc';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP FUNCTION FUN_getAgeStr';
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
    EXECUTE IMMEDIATE 'DROP TABLE IndexT';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE "PCMS"."tb_emp6"';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE "PCMS"."tb_dept1"';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE "PCMS"."info"';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE "PCMS"."CUSTOMER"';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE "PCMS"."CUSTOMER1"';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE range_hash_Partition_Table';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE hash_Partition_Table';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE list_Partition_Table';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE range_Partition_Table';
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
    EXECUTE IMMEDIATE 'DROP TABLE testBin';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testString';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testTime';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testBit';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testFloat';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE testInt';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

-- Test basic data types
CREATE TABLE testInt(
    f1 NUMBER(3),
    f2 NUMBER(5),
    f3 NUMBER(8),
    f4 NUMBER(10),
    f5 NUMBER(5),
    f6 NUMBER(10),
    f7 NUMBER(19)
);
CREATE INDEX idx_testInt_1 ON testInt(f1);
INSERT INTO testInt(f1,f2,f3,f4,f5,f6,f7) VALUES(1,2,3,4,5,6,7);

CREATE TABLE testFloat(
    f1 FLOAT,
    f2 FLOAT(5),
    f3 BINARY_DOUBLE,
    f4 BINARY_DOUBLE
);
CREATE INDEX idx_testFloat_1 ON testFloat(f1);
INSERT INTO testFloat(f1,f2,f3,f4) VALUES(123.45,123.45,123.45,12.456);

CREATE TABLE testBit(
    f1 NUMBER(1),
    f2 NUMBER(5),
    f3 NUMBER(19)
);
CREATE INDEX idx_testBit_1 ON testBit(f1);
INSERT INTO testBit VALUES(1,31,65);

-- Note: Oracle doesn't have BIT type, use NUMBER instead
SELECT f1, f2, f3 FROM testBit;

CREATE TABLE testTime(
    f1 NUMBER(4),
    f2 NUMBER(4),
    f3 DATE,
    f4 INTERVAL DAY TO SECOND,
    f5 TIMESTAMP,
    f6 TIMESTAMP
);
CREATE INDEX idx_testTime_1 ON testTime(f1);
INSERT INTO testTime(f1,f2,f3,f5,f6) VALUES(2022,2022,TO_DATE('2022-07-12','YYYY-MM-DD'),TO_TIMESTAMP('2022-07-12 14:53:00','YYYY-MM-DD HH24:MI:SS'),TO_TIMESTAMP('2022-07-12 14:54:00','YYYY-MM-DD HH24:MI:SS'));

CREATE TABLE testString(
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
CREATE INDEX idx_testString_1 ON testString(f1);
INSERT INTO testString(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('1','abcde','abc123','product_data_batch_001','database_transaction_log_20220712','checksum_validation_report','d','aa,bb');

CREATE TABLE testBin(
    f1 RAW(1),
    f2 RAW(3),
    f3 RAW(10),
    f4 BLOB,
    f5 BLOB,
    f6 BLOB,
    f7 BLOB
);
CREATE INDEX idx_testBin_1 ON testBin(f1);
INSERT INTO testBin(f1,f2,f3,f4,f5,f6,f7) VALUES('61','616263','6162642e31323334','01010101','9023123123','database_checksum_data','validation_result_data');

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
SELECT * FROM tmp_account WHERE acct_num=150;

INSERT INTO account VALUES(200,13.23);
UPDATE account SET acct_num = 201 WHERE amount = 13.23;
SELECT * FROM tmp_account;

INSERT INTO account VALUES(300,14.23);
DELETE FROM account WHERE acct_num = 300;
SELECT * FROM tmp_account;

-- Test partition tables
CREATE TABLE range_Partition_Table(
    range_key_column DATE,
    NAME VARCHAR2(20),
    ID NUMBER
) PARTITION BY RANGE(range_key_column)(
    PARTITION PART_202007 VALUES LESS THAN (TO_DATE('2020-07-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),
    PARTITION PART_202008 VALUES LESS THAN (TO_DATE('2020-08-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),
    PARTITION PART_202009 VALUES LESS THAN (TO_DATE('2020-09-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))
);

CREATE TABLE "PCMS"."CUSTOMER"(
    CUSTOMER_ID NUMBER NOT NULL PRIMARY KEY,
    FIRST_NAME  VARCHAR2(30) NOT NULL,
    LAST_NAME   VARCHAR2(30) NOT NULL,
    PHONE       VARCHAR2(15) NOT NULL,
    EMAIL       VARCHAR2(80),
    STATUS      CHAR(1)
) PARTITION BY RANGE (CUSTOMER_ID)(
    PARTITION CUS_PART1 VALUES LESS THAN (100000),
    PARTITION CUS_PART2 VALUES LESS THAN (200000)
);

CREATE TABLE "PCMS"."CUSTOMER1"(
    CUSTOMER_ID VARCHAR2(30) NOT NULL,
    FIRST_NAME  VARCHAR2(30) NOT NULL,
    LAST_NAME   VARCHAR2(30) NOT NULL,
    PHONE       VARCHAR2(15) NOT NULL,
    EMAIL       VARCHAR2(80),
    STATUS      CHAR(1)
) PARTITION BY LIST (CUSTOMER_ID)(
    PARTITION CUS_PART1 VALUES LESS THAN ('100000'),
    PARTITION CUS_PART2 VALUES LESS THAN ('200000')
);

CREATE TABLE list_Partition_Table(
    NAME VARCHAR2(10),
    DATA VARCHAR2(20)
) PARTITION BY LIST(NAME)(
    PARTITION PART_01 VALUES ('ME','PE','QC','RD'),
    PARTITION PART_02 VALUES ('SMT','SALE')
);

CREATE TABLE hash_Partition_Table(
    hash_key_column VARCHAR2(30),
    DATA VARCHAR2(20)
) PARTITION BY HASH(hash_key_column)(
    PARTITION PART_0001,
    PARTITION PART_0002,
    PARTITION PART_0003,
    PARTITION PART_0004
);

CREATE TABLE range_hash_Partition_Table(
    range_column_key DATE,
    hash_column_key NUMBER,
    DATA VARCHAR2(20)
) PARTITION BY RANGE(range_column_key)
    SUBPARTITION BY HASH(hash_column_key) SUBPARTITIONS 2(
        PARTITION PART_1990 VALUES LESS THAN (TO_DATE('1990-01-01','YYYY-MM-DD')),
        PARTITION PART_2000 VALUES LESS THAN (TO_DATE('2000-01-01','YYYY-MM-DD')),
        PARTITION PART_MAXVALUE VALUES LESS THAN (MAXVALUE)
);

-- Test foreign key constraints
CREATE TABLE "PCMS"."tb_dept1" (
    "ID" NUMBER(11) NOT NULL,
    "NAME" VARCHAR2(22) NOT NULL,
    "LOCATION" VARCHAR2(50),
    PRIMARY KEY ("ID")
);

CREATE TABLE "PCMS"."tb_emp6" (
    "id" NUMBER(11,0) NOT NULL,
    "name" VARCHAR2(25 BYTE),
    "deptId" NUMBER(11,0),
    "salary" FLOAT(126),
    PRIMARY KEY ("id")
);
ALTER TABLE "PCMS"."tb_emp6" ADD CONSTRAINT "fk_emp_dept1" FOREIGN KEY ("deptId") REFERENCES "PCMS"."tb_dept1" ("ID");

-- Test stored procedures and functions
CREATE OR REPLACE FUNCTION FUN_getAgeStr(age IN NUMBER)
RETURN VARCHAR2 IS
    results VARCHAR2(20);
BEGIN
    IF age <= 14 THEN
        results := '儿童';
    ELSIF age <= 24 THEN
        results := '青少年';
    ELSIF age <= 44 THEN
        results := '青年';
    ELSIF age <= 59 THEN
        results := '中年';
    ELSE
        results := '老年';
    END IF;
    RETURN results;
END;
/

-- Create info table for testing
CREATE TABLE "PCMS"."info" (
    "ID" NUMBER NOT NULL,
    "AGE" NUMBER NOT NULL,
    "ADDRESS" VARCHAR2(50) NOT NULL,
    "SALARY" NUMBER(10,2) NOT NULL,
    PRIMARY KEY ("ID")
);

INSERT INTO "PCMS"."info"(ID,AGE,ADDRESS,SALARY) VALUES(1,32,'Beijing Financial District',2000.00);
INSERT INTO "PCMS"."info"(ID,AGE,ADDRESS,SALARY) VALUES(2,25,'Shanghai Pudong New Area',1500.00);
INSERT INTO "PCMS"."info"(ID,AGE,ADDRESS,SALARY) VALUES(3,23,'Hangzhou West Lake District',2000.00);
INSERT INTO "PCMS"."info"(ID,AGE,ADDRESS,SALARY) VALUES(4,25,'Henan Zhengzhou High-tech Zone',6500.00);
INSERT INTO "PCMS"."info"(ID,AGE,ADDRESS,SALARY) VALUES(5,27,'Hunan Changsha Economic Zone',8500.00);
INSERT INTO "PCMS"."info"(ID,AGE,ADDRESS,SALARY) VALUES(6,22,'Hunan Xiangtan Industrial Park',4500.00);
INSERT INTO "PCMS"."info"(ID,AGE,ADDRESS,SALARY) VALUES(7,24,'Hebei Shijiazhuang Development Zone',10000.00);

CREATE OR REPLACE PROCEDURE countproc(sid IN NUMBER, num OUT NUMBER) IS
BEGIN
    SELECT COUNT(*) INTO num FROM "PCMS"."info" WHERE salary > 5000;
END;
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
CREATE TABLE IndexT(
    "id" NUMBER(11) NOT NULL,
    "tenantry_id" NUMBER(20) NOT NULL,
    "code" VARCHAR2(64) NOT NULL,
    "goods_name" VARCHAR2(50) NOT NULL,
    "props_name" VARCHAR2(100) NOT NULL,
    "price" NUMBER(10,2) NOT NULL,
    "price_url" VARCHAR2(1000) NOT NULL,
    "create_time" TIMESTAMP NOT NULL,
    "modify_time" TIMESTAMP DEFAULT NULL,
    "deleted" NUMBER(1) NOT NULL DEFAULT 0,
    PRIMARY KEY ("id")
);
CREATE INDEX "idx_IndexT_2" ON IndexT("tenantry_id","code");
CREATE INDEX "idx_IndexT_3" ON IndexT("code","tenantry_id");

-- Add comments to the IndexT table
COMMENT ON TABLE IndexT IS 'Product information table';
COMMENT ON COLUMN IndexT.tenantry_id IS 'Product ID';
COMMENT ON COLUMN IndexT.code IS 'Product SKU number';
COMMENT ON COLUMN IndexT.goods_name IS 'Product name';
COMMENT ON COLUMN IndexT.props_name IS 'Product attributes string format: p1:v1;p2:v2';
COMMENT ON COLUMN IndexT.price IS 'Product pricing';
COMMENT ON COLUMN IndexT.price_url IS 'Product main image URL';
COMMENT ON COLUMN IndexT.create_time IS 'Product creation time';
COMMENT ON COLUMN IndexT.modify_time IS 'Product last modification time';
COMMENT ON COLUMN IndexT.deleted IS 'Logical deletion flag';