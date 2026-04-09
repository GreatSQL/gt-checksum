-- to be applied in target MySQL instance
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS=0;
SET UNIQUE_CHECKS=0;
/*!80013 SET SESSION sql_require_primary_key=0 */;
/*!80030 SET SESSION sql_generate_invisible_primary_key=0 */;

/*!40000 DROP DATABASE IF EXISTS `gt_checksum`*/;
CREATE DATABASE /*!32312 IF NOT EXISTS*/ `gt_checksum` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*!80000 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE gt_checksum;

-- 测试几个基本数据类型
DROP TABLE IF EXISTS testint;
CREATE TABLE testint(
    f1 TINYINT,
    f2 SMALLINT NOT NULL,
    f3 MEDIUMINT NOT NULL DEFAULT 3,
    f4 INT UNSIGNED DEFAULT 4,
    f5 INT(11) ZEROFILL NOT NULL,
    f6 INT(3) UNSIGNED ZEROFILL,
    f7 BIGINT
) COMMENT='table testint' ;
ALTER TABLE testint ADD INDEX idx_testint_1(f1);
INSERT INTO testint(f1,f2,f3,f4,f5,f6,f7) VALUES(1,2,3,4,5,6,7);

DROP TABLE IF EXISTS testfloat;
CREATE TABLE testfloat(
  f1 FLOAT,
  f2 FLOAT(5,2),
  f3 DOUBLE,
  f4 DOUBLE(5,3)
);
ALTER TABLE testfloat ADD INDEX idx_testfloat_1(f1);
INSERT INTO testfloat(f1,f2,f3,f4) VALUES(123.45,123.45,123.45,12.456);

DROP TABLE IF EXISTS testbit;
CREATE TABLE testbit(
    f1 BIT,
    f2 BIT(5),
    f3 BIT(64)
);
ALTER TABLE testbit ADD INDEX idx_testbit_1(f1);
INSERT INTO testbit VALUES(1,31,65);

/*!80030 SET SESSION sql_generate_invisible_primary_key=1 */;
DROP TABLE IF EXISTS testtime;
CREATE TABLE testtime(
     f1 YEAR,
     f2 YEAR(4) DEFAULT 2026,
     f3 DATE DEFAULT '2026-03-16',
     f4 TIME DEFAULT '15:15:30',
     f5 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     f6 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE testtime ADD INDEX idx_testtime_1(f1);
INSERT INTO testtime(f1,f2,f3,f4,f5,f6) VALUES('2022',2022,'2022-07-12','12:30:29','2022-07-12 14:53:00','2022-07-12 14:54:00');

DROP TABLE IF EXISTS teststring;
CREATE TABLE teststring(
   f1 CHAR,
   f2 VARCHAR(5),
   f3 CHAR(15),
   f4 TINYTEXT,
   f5 TEXT,
   f6 MEDIUMTEXT,
   f7 LONGTEXT,
   f8 ENUM('a','b','c','d'),
   f9 SET('aa','bb','cc','dd')
);
ALTER TABLE teststring ADD INDEX idx_teststring_1(f1);
INSERT INTO teststring(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('1','abcde','abc123','abcd.1234','hello gt-checksum','hello ','hello gt-checksum','a','aa,bb');
INSERT INTO teststring(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('3','klmno','ghi789','ijkl.9012',"a\\\b\\'c",'hello ','hello gt-checksum','c','cc,dd');

-- 测试view
create view v_teststring as select * from teststring where f1<='3';

DROP TABLE IF EXISTS testbin;
CREATE TABLE testbin(
    f1 BINARY,
    f2 BINARY(3),
    f3 VARBINARY(10),
    f4 TINYBLOB,
    f5 BLOB,
    f6 MEDIUMBLOB,
    f7 LONGBLOB
);
ALTER TABLE testbin ADD INDEX idx_testbin_1(f1);
INSERT INTO testbin(f1,f2,f3,f4,f5,f6,f7) VALUES('a','abc','abcd.1234','01010101','0x9023123123','hello gt-checksum','hello gt-checksum');

/*!80030 SET SESSION sql_generate_invisible_primary_key=0 */;
-- 测试触发器的处理
DROP TABLE IF EXISTS account;
CREATE TABLE account (
    acct_num INT, 
    amount DECIMAL(10,2)
);

-- 创建影子表
DROP TABLE IF EXISTS tmp_account;
CREATE TABLE tmp_account (
    acct_num INT, 
    amount DECIMAL(10,2),
    sql_text VARCHAR(100)
);

-- 注意：针对account表，创建触发器，会造成第一次校验并修复后，因为触发器导致tmp_account表数据发生变化
--      修复结束后再次校验报告tmp_account表数据不一致，这种情况下需要先把触发器删除
-- 监控insert
DELIMITER ||
DROP TRIGGER IF EXISTS accountInsert;
CREATE TRIGGER accountInsert BEFORE INSERT
    ON account FOR EACH ROW
BEGIN
    INSERT INTO tmp_account VALUES(NEW.acct_num,NEW.amount,"INSERT-x");
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

-- 测试分区
DROP TABLE IF EXISTS range_partition_table;
CREATE TABLE range_partition_table(
    range_key_column DATETIME,
    name VARCHAR(20),
    id INT
) PARTITION BY RANGE(to_days(range_key_column))(
    PARTITION PART_202007 VALUES LESS THAN (to_days('2020-07-1')),
    PARTITION PART_202008 VALUES LESS THAN (to_days('2020-08-1')),
    PARTITION PART_202009 VALUES LESS THAN (to_days('2020-09-1'))
);

DROP TABLE IF EXISTS customer;
CREATE TABLE customer(
    customer_id INT NOT NULL PRIMARY KEY,
    first_name  VARCHAR(30) NOT NULL,
    last_name   VARCHAR(30) NOT NULL,
    phone       VARCHAR(15) NOT NULL,
    email       VARCHAR(80),
    status      CHAR(1)
)PARTITION BY RANGE (customer_id)(
 PARTITION CUS_PART1 VALUES LESS THAN (100000),
 PARTITION CUS_PART2 VALUES LESS THAN (200000),
 PARTITION CUS_PART3 VALUES LESS THAN (300000),
 PARTITION CUS_PART4 VALUES LESS THAN (400000)
);

DROP TABLE IF EXISTS customer1;
CREATE TABLE customer1(
    customer_id BIGINT NOT NULL,
    first_name  VARCHAR(30) NOT NULL,
    last_name   VARCHAR(30) NOT NULL,
    phone       VARCHAR(15) NOT NULL,
    email       VARCHAR(80),
    status      CHAR(1)
) PARTITION BY RANGE COLUMNS (customer_id)(
    PARTITION CUS_PART1 VALUES LESS THAN (100000)
);

DROP TABLE IF EXISTS list_partition_table;
CREATE TABLE list_partition_table(
    name VARCHAR(10),
    data VARCHAR(20)
)PARTITION BY LIST COLUMNS (name)(
    PARTITION PART_01 VALUES IN ('ME','PE','QC','RD'),
    PARTITION PART_02 VALUES IN ('SMT','SALE')
);

DROP TABLE IF EXISTS hash_partition_table;
CREATE TABLE hash_partition_table(
    hash_key_column INT(30),
    data VARCHAR(20)
) PARTITION BY HASH (hash_key_column)
PARTITIONS 4;

DROP TABLE IF EXISTS range_hash_partition_table;
CREATE TABLE range_hash_partition_table (
    id INT,
    purchased DATE,
    data VARCHAR(20),
    purchase_year INT,
    purchase_day_of_year VARCHAR(3)
    )
    PARTITION BY RANGE( YEAR(purchased) )
    SUBPARTITION BY HASH( TO_DAYS(purchased) )
    SUBPARTITIONS 2 (
        PARTITION p0 VALUES LESS THAN (1990),
        PARTITION p1 VALUES LESS THAN (2000),
        PARTITION p2 VALUES LESS THAN MAXVALUE
);

/*!80030 SET SESSION sql_generate_invisible_primary_key=1 */;
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
    deptid INT(11),
    salary FLOAT,
    CONSTRAINT fk_emp_dept1
    FOREIGN KEY(deptid) REFERENCES tb_dept1(id)
);

-- 测试存储程序
DELIMITER ||
DROP FUNCTION IF EXISTS getAgeStr;
CREATE FUNCTION getAgeStr(age INT)
RETURNS VARCHAR(20)
DETERMINISTIC
NO SQL
COMMENT 'FUNCTION getAgeStr'
BEGIN
	DECLARE results VARCHAR(20);
	IF age<=14 then
		set results = 'Children';
	ELSEIF age <=25 THEN
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

DELIMITER ||
DROP PROCEDURE IF EXISTS myAdd;
CREATE PROCEDURE myAdd(IN n1 INT, IN n2 INT, OUT s INT)
COMMENT 'PROCEDURE myAdd'
BEGIN
    SET s = n1 + n2 + 1;
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
  INSERT INTO test2 SET a2=NEW.a1 + 1;
END ||
DELIMITER ;

/*!80030 SET SESSION sql_generate_invisible_primary_key=0 */;
-- 测试索引
DROP TABLE IF EXISTS indext;
CREATE TABLE indext(
    `id` INT(11) NOT NULL AUTO_INCREMENT,
    `tenantry_id` BIGINT(20) NOT NULL,
    `code` VARCHAR(64) NOT NULL COMMENT 'col code',
    `goods_name` VARCHAR(50) NOT NULL,
    `props_name` VARCHAR(100) NOT NULL,
    `price` DECIMAL(10,2) NOT NULL,
    `price_url` VARCHAR(1000) NOT NULL,
    `create_time` DATETIME NOT NULL,
    `modify_time` DATETIME DEFAULT NULL,
    `deleted` TINYINT(1) NOT NULL DEFAULT '0',
    `商品描述` VARCHAR(190) DEFAULT NULL COMMENT '商品描述',
    PRIMARY KEY (`id`),
    KEY `idx_2` (`tenantry_id`,`code`),
    KEY `idx 3` (`code`,`tenantry_id`),
    KEY `idx_4` (goods_name),  -- 测试前缀索引/部分索引
    KEY `idx_5` (`price`),  -- 测试函数索引
    KEY `中文索引` (`商品描述`)
) ENGINE=InnoDB AUTO_INCREMENT=10 COMMENT 'table indext';
INSERT INTO indext VALUES ('583532949','8674665223082153551','aut','animi','eum','1.99','fugit','2026-02-17 16:04:25','2025-06-20 22:10:41','1','高品质商品');
INSERT INTO indext VALUES ('914246705','2020683354385918016','quam','aut','cumque','0.00','nihil','2025-03-20 01:01:33','2025-07-27 22:10:28','2','普通商品');

-- 测试从Oracle=>MySQL数据同步
CREATE TABLE t1 (
    id BIGINT NOT NULL AUTO_INCREMENT,
    c_varchar2 VARCHAR(4000),
    c_char VARCHAR(10),
    c_nchar VARCHAR(10),
    c_nvarchar2 VARCHAR(1000),
    c_number DECIMAL(38,5),
    c_float DOUBLE,
    c_decimal DECIMAL(10,2),
    c_date DATETIME,
    c_timestamp DATETIME(6),
    c_clob LONGTEXT,
    c_boolean TINYINT(1),
    PRIMARY KEY (id)
);

-- 1. 常规标准数据
INSERT INTO t1 VALUES (
    1,
    'Standard English Text',
    'A',
    'NCHAR值',
    'NVARCHAR2标准文本',
    12345.6789,
    123.456,
    99.99,
    '2023-10-01 12:30:00',
    '2023-10-01 12:30:00.123456',
    'Standard CLOB text data.',
    1
);

-- 2. 边界值与特殊字符 (包含Emoji、极大极小值、年份极值)
INSERT INTO t1 VALUES(
    2,
    'Special chars: ~!@#$%^&*()_+{}|:"<>? / 汉字 / 🚀',
    'CHAR10    ',
    '测试        ',
    '多语言: こんにちは, 안녕하세요, 🚀',
    '999999999999999999999999999999999.99999',
    '9007199254740991',
    '-99999999.99',
    '9999-12-31 23:59:59',
    '1970-01-01 00:00:00',
    'AAAAAAAAAABBBBBBBBBB',
    0
);

-- 3. 全 NULL 值测试 (主键除外)
-- 这条语句的语法在 Oracle 和 MySQL 中是完全一致的
INSERT INTO t1 (id) VALUES (3);
COMMIT;
