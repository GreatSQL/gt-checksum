-- Oracle Test Case Script for gt-checksum

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
    EXECUTE IMMEDIATE 'DROP PROCEDURE countproc';
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
    EXECUTE IMMEDIATE 'DROP TABLE IndexT';
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
    EXECUTE IMMEDIATE 'DROP TABLE CUSTOMER';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE CUSTOMER1';
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

-- Create user and grant privileges (if needed)
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
INSERT INTO testString(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('1','abcde','abc123','abcd.1234','hello gt-checksum','hello ','hello gt-checksum','a','aa,bb');
INSERT INTO testString(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('2','fghij','def456','efgh.5678','hello, i''m gt-checksum','hello ','hello gt-checksum','b','cc,dd');
INSERT INTO testString(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('3','klmno','ghi789','ijkl.9012','a\b''c','hello ','hello gt-checksum','c','cc,dd');

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
INSERT INTO testBin(f1,f2,f3,f4,f5,f6,f7) 
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
CREATE TABLE range_Partition_Table(
    range_key_column DATE,
    NAME VARCHAR2(20),
    ID NUMBER
) PARTITION BY RANGE(range_key_column)(
    PARTITION PART_202007 VALUES LESS THAN (TO_DATE('2020-07-01','YYYY-MM-DD')),
    PARTITION PART_202008 VALUES LESS THAN (TO_DATE('2020-08-01','YYYY-MM-DD')),
    PARTITION PART_202009 VALUES LESS THAN (TO_DATE('2020-09-01','YYYY-MM-DD'))
);

CREATE TABLE CUSTOMER(
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

CREATE TABLE CUSTOMER1(
    CUSTOMER_ID NUMBER NOT NULL,
    FIRST_NAME  VARCHAR2(30) NOT NULL,
    LAST_NAME   VARCHAR2(30) NOT NULL,
    PHONE       VARCHAR2(15) NOT NULL,
    EMAIL       VARCHAR2(80),
    STATUS      CHAR(1)
) PARTITION BY RANGE (CUSTOMER_ID)(
    PARTITION CUS_PART1 VALUES LESS THAN (100000),
    PARTITION CUS_PART2 VALUES LESS THAN (200000)
);

CREATE TABLE list_Partition_Table(
    NAME VARCHAR2(10),
    DATA VARCHAR2(20)
) PARTITION BY LIST(NAME)(
    PARTITION PART_01 VALUES ('ME','PE','QC','RD'),
    PARTITION PART_02 VALUES ('SMT','SALE')
);

CREATE TABLE hash_Partition_Table(
    hash_key_column NUMBER(30),
    DATA VARCHAR2(20)
) PARTITION BY HASH(hash_key_column)
PARTITIONS 4;

CREATE TABLE range_hash_Partition_Table (
    id NUMBER,
    purchased DATE,
    DATA VARCHAR2(20),
    purchase_year AS (EXTRACT(YEAR FROM purchased)) VIRTUAL,
    purchase_day_of_year AS (TO_CHAR(purchased, 'DDD')) VIRTUAL
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
    deptId NUMBER(11),
    salary FLOAT,
    CONSTRAINT fk_emp_dept1
    FOREIGN KEY(deptId) REFERENCES tb_dept1(id)
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
CREATE TABLE IndexT(
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
CREATE INDEX idx_IndexT_2 ON IndexT(tenantry_id,code);
CREATE INDEX idx_IndexT_3 ON IndexT(code,tenantry_id);
