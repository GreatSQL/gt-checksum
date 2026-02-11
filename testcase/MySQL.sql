SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS=0;
SET UNIQUE_CHECKS=0;
SET sql_generate_invisible_primary_key=OFF;

DROP DATABASE IF EXISTS gt_checksum;
CREATE DATABASE IF NOT EXISTS gt_checksum;
USE gt_checksum;

-- 测试几个基本数据类型
DROP TABLE IF EXISTS testInt;
CREATE TABLE testInt(
    f1 TINYINT,
    f2 SMALLINT,
    f3 MEDIUMINT,
    f4 INT,
    f5 INT(5) ZEROFILL,
    f6 INT UNSIGNED,
    f7 BIGINT
);
ALTER TABLE testInt ADD INDEX idx_testInt_1(f1);
INSERT INTO testInt(f1,f2,f3,f4,f5,f6,f7) VALUES(1,2,3,4,5,6,7);

DROP TABLE IF EXISTS testFloat;
CREATE TABLE testFloat(
  f1 FLOAT,
  f2 FLOAT(5,2),
  f3 DOUBLE,
  f4 DOUBLE(5,3)
);
ALTER TABLE testFloat ADD INDEX idx_testFloat_1(f1);
INSERT INTO testFloat(f1,f2,f3,f4) VALUES(123.45,123.45,123.45,12.456);

DROP TABLE IF EXISTS testBit;
CREATE TABLE testBit(
    f1 BIT,
    f2 BIT(5),
    f3 BIT(64)
);
ALTER TABLE testBit ADD INDEX idx_testBit_1(f1);
INSERT INTO testBit VALUES(1,31,65);

DROP TABLE IF EXISTS testTime;
CREATE TABLE testTime(
     f1 YEAR,
     f2 YEAR(4),
     f3 DATE,
     f4 TIME,
     f5 DATETIME,
     f6 TIMESTAMP
);
ALTER TABLE testTime ADD INDEX idx_testTime_1(f1);
INSERT INTO testTime(f1,f2,f3,f4,f5,f6) VALUES('2022',2022,'2022-07-12','2 12:30:29','2022-07-12 14:53:00','2022-07-12 14:54:00');

DROP TABLE IF EXISTS testString;
CREATE TABLE testString(
   f1 CHAR,
   f2 CHAR(5),
   f3 VARCHAR(10),
   f4 TINYTEXT,
   f5 TEXT,
   f6 MEDIUMTEXT,
   f7 LONGTEXT,
   f8 ENUM('a','b','c','d'),
   f9 SET('aa','bb','cc','dd')
);
ALTER TABLE testString ADD INDEX idx_testString_1(f1);
INSERT INTO testString(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('1','abcde','abc123','abcd.1234','hello gt-checksum','hello ','hello gt-checksum','a','aa,bb');
INSERT INTO testString(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('2','fghij','def456','efgh.5678',"hello, i\'m gt-checksum",'hello ','hello gt-checksum','b','cc,dd');
INSERT INTO testString(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('3','klmno','ghi789','ijkl.9012',concat("a\\\b\\'c",repeat(chr(rand()*102),5)),'hello ','hello gt-checksum','c','cc,dd');

DROP TABLE IF EXISTS testBin;
CREATE TABLE testBin(
    f1 BINARY,
    f2 BINARY(3),
    f3 VARBINARY(10),
    f4 TINYBLOB,
    f5 BLOB,
    f6 MEDIUMBLOB,
    f7 LONGBLOB
);
ALTER TABLE testBin ADD INDEX idx_testBin_1(f1);
INSERT INTO testBin(f1,f2,f3,f4,f5,f6,f7) VALUES('a','abc','abcd.1234','01010101','0x9023123123','hello gt-checksum','hello gt-checksum');

-- 测试触发器的处理
DROP TABLE IF EXISTS account;
CREATE TABLE account (
    acct_num INT, 
    amount DECIMAL(10,2)
);
INSERT INTO account VALUES(137,14.98),(141,1937.50),(97,-100.00);

-- 创建影子表
DROP TABLE IF EXISTS tmp_account;
CREATE TABLE tmp_account (
    acct_num INT, 
    amount DECIMAL(10,2),
    sql_text VARCHAR(100)
);

-- 监控insert
DELIMITER ||
DROP TRIGGER IF EXISTS accountInsert;
CREATE TRIGGER accountInsert BEFORE INSERT
    ON account FOR EACH ROW
BEGIN
    INSERT INTO tmp_account VALUES(NEW.acct_num,NEW.amount,"INSERT");
END ||

-- 监控delete
DELIMITER ||
DROP TRIGGER IF EXISTS accountDelete;
CREATE TRIGGER accountDelete BEFORE DELETE
    ON account FOR EACH ROW
BEGIN
    INSERT INTO tmp_account VALUES(OLD.acct_num,OLD.amount,"DELETE");
END ||

-- 监控update
DELIMITER ||
DROP TRIGGER IF EXISTS accountUpdate;
CREATE TRIGGER accountUpdate BEFORE UPDATE
    ON account FOR EACH ROW
BEGIN
    INSERT INTO tmp_account VALUES(OLD.acct_num,OLD.amount,"UPDATE_DELETE");
    INSERT INTO tmp_account VALUES(NEW.acct_num,NEW.amount,"UPDATE_INSERT");
END ||
DELIMITER ;

INSERT INTO account VALUES (150,33.32);

INSERT INTO account VALUES(200,13.23);
UPDATE account SET acct_num = 201 WHERE amount = 13.23;

INSERT INTO account VALUES(300,14.23);
DELETE FROM account WHERE acct_num = 300;

-- 测试分区
DROP TABLE IF EXISTS range_Partition_Table;
CREATE TABLE range_Partition_Table(
    range_key_column DATETIME,
    NAME VARCHAR(20),
    ID INT
) PARTITION BY RANGE(to_days(range_key_column))(
    PARTITION PART_202007 VALUES LESS THAN (to_days('2020-07-1')),
    PARTITION PART_202008 VALUES LESS THAN (to_days('2020-08-1')),
    PARTITION PART_202009 VALUES LESS THAN (to_days('2020-09-1'))
);

DROP TABLE IF EXISTS gt_checksum.CUSTOMER;
CREATE TABLE gt_checksum.CUSTOMER(
    CUSTOMER_ID INT NOT NULL PRIMARY KEY,
    FIRST_NAME  VARCHAR(30) NOT NULL,
    LAST_NAME   VARCHAR(30) NOT NULL,
    PHONE       VARCHAR(15) NOT NULL,
    EMAIL       VARCHAR(80),
    STATUS      CHAR(1)
)PARTITION BY RANGE (CUSTOMER_ID)(
 PARTITION CUS_PART1 VALUES LESS THAN (100000),
 PARTITION CUS_PART2 VALUES LESS THAN (200000)
);

DROP TABLE IF EXISTS gt_checksum.CUSTOMER1;
CREATE TABLE gt_checksum.CUSTOMER1(
    CUSTOMER_ID BIGINT NOT NULL,
    FIRST_NAME  VARCHAR(30) NOT NULL,
    LAST_NAME   VARCHAR(30) NOT NULL,
    PHONE       VARCHAR(15) NOT NULL,
    EMAIL       VARCHAR(80),
    STATUS      CHAR(1)
) PARTITION BY RANGE COLUMNS (CUSTOMER_ID)(
    PARTITION CUS_PART1 VALUES LESS THAN (100000),
    PARTITION CUS_PART2 VALUES LESS THAN (200000)
);

DROP TABLE IF EXISTS list_Partition_Table;
CREATE TABLE list_Partition_Table(
    NAME VARCHAR(10),
    DATA VARCHAR(20)
)PARTITION BY LIST COLUMNS (NAME)(
    PARTITION PART_01 VALUES IN ('ME','PE','QC','RD'),
    PARTITION PART_02 VALUES IN ('SMT','SALE')
);

DROP TABLE IF EXISTS hash_Partition_Table;
CREATE TABLE hash_Partition_Table(
    hash_key_column INT(30),
    DATA VARCHAR(20)
) PARTITION BY HASH (hash_key_column)
PARTITIONS 4;

DROP TABLE IF EXISTS range_hash_Partition_Table;
CREATE TABLE range_hash_Partition_Table (id INT, purchased DATE)
    PARTITION BY RANGE( YEAR(purchased) )
    SUBPARTITION BY HASH( TO_DAYS(purchased) )
    SUBPARTITIONS 2 (
        PARTITION p0 VALUES LESS THAN (1990),
        PARTITION p1 VALUES LESS THAN (2000),
        PARTITION p2 VALUES LESS THAN MAXVALUE
);

-- 测试外键约束
DROP TABLE IF EXISTS tb_dept1;
CREATE TABLE tb_dept1 (
  id INT(11) PRIMARY KEY,
  name VARCHAR(22) NOT NULL,
  location VARCHAR(50)
);

DROP TABLE IF EXISTS tb_emp6;
CREATE TABLE tb_emp6(
    id INT(11) PRIMARY KEY,
    name VARCHAR(25),
    deptId INT(11),
    salary FLOAT,
    CONSTRAINT fk_emp_dept1
    FOREIGN KEY(deptId) REFERENCES tb_dept1(id)
);

-- 测试存储程序
DELIMITER ||
DROP FUNCTION IF EXISTS getAgeStr;
CREATE FUNCTION getAgeStr(age INT)
RETURNS VARCHAR(20)
DETERMINISTIC
NO SQL
BEGIN
	DECLARE results VARCHAR(20);
	IF age<=14 then
		set results = 'Children';
	ELSEIF age <=24 THEN
		set results = 'Teenagers';
	ELSEIF age <=44 THEN
		set results = 'Youth';
	ELSEIF age <=59 THEN
		set results = 'Middle Age';
ELSE
		SET results = 'Elderly';
END IF;
RETURN results;
END ||
DELIMITER ;

-- 再次测试触发器
DROP TABLE IF EXISTS test1;
CREATE TABLE test1(
    a1 INT
);
DROP TABLE IF EXISTS test2;
CREATE TABLE test2(
    a2 INT
);
DELIMITER ||
DROP TRIGGER IF EXISTS tri_test;
CREATE TRIGGER tri_test
 BEFORE INSERT ON test1 FOR EACH ROW BEGIN
  INSERT INTO test2 SET a2=NEW.a1;
END ||
DELIMITER ;

-- 测试索引
DROP TABLE IF EXISTS IndexT;
CREATE TABLE IndexT(
    `id` INT(11) NOT NULL,
    `tenantry_id` BIGINT(20) NOT NULL,
    `code` VARCHAR(64) NOT NULL,
    `goods_name` VARCHAR(50) NOT NULL,
    `props_name` VARCHAR(100) NOT NULL,
    `price` DECIMAL(10,2) NOT NULL,
    `price_url` VARCHAR(1000) NOT NULL,
    `create_time` DATETIME NOT NULL,
    `modify_time` DATETIME DEFAULT NULL,
    `deleted` TINYINT(1) NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`),
    KEY `idx_2` (`tenantry_id`,`code`),
    KEY `idx_3` (`code`,`tenantry_id`)
) ENGINE=InnoDB;
