SET sql_generate_invisible_primary_key=OFF;

DROP DATABASE IF EXISTS gt_checksum;
CREATE DATABASE IF NOT EXISTS gt_checksum;
USE gt_checksum;

-- 测试数值类型
DROP TABLE IF EXISTS testInt;
CREATE TABLE testInt(
    f1 TINYINT,
    f2 SMALLINT,
    f3 MEDIUMINT,
    f4 INT,
    f5 INT(5) ZEROFILL,
    f6 INT UNSIGNED,
    f7 BIGINT
) CHARACTER SET 'utf8';
ALTER TABLE testInt ADD INDEX idx_1(f1);
INSERT INTO testInt(f1,f2,f3,f4,f5,f6,f7) VALUES(1,2,3,4,5,6,7);

DROP TABLE IF EXISTS testFlod;
CREATE TABLE testFlod(
  f1 FLOAT,
  f2 FLOAT(5,2),
  f3 DOUBLE,
  f4 DOUBLE(5,3)
) CHARACTER SET 'utf8';
ALTER TABLE testFlod ADD INDEX idx_1(f1);
INSERT INTO testFlod(f1,f2,f3,f4) VALUES(123.45,123.45,123.45,12.456);

-- 测试二进制类型
DROP TABLE IF EXISTS testBit;
CREATE TABLE testBit(
    f1 BIT,
    f2 BIT(5),
    f3 BIT(64)
) CHARACTER SET 'utf8';
ALTER TABLE testBit ADD INDEX idx_1(f1);
INSERT INTO testBit VALUES(1,31,65);

-- from bin,oct,hex bin转换为二进制，oct8进制，hex16进制
SELECT * FROM testBit;

-- 测试时间类型
DROP TABLE IF EXISTS testTime;
CREATE TABLE testTime(
     f1 YEAR,
     f2 YEAR(4),
     f3 DATE,
     f4 TIME,
     f5 DATETIME,
     f6 TIMESTAMP
) CHARACTER SET 'utf8';
ALTER TABLE testTime ADD INDEX idx_1(f1);
INSERT INTO testTime(f1,f2,f3,f4,f5,f6) VALUES('2022',2022,'2022-07-12','2 12:30:29','2022-07-12 14:53:00','2022-07-12 14:54:00');

-- 测试字符串类型
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
) CHARACTER SET 'utf8';
ALTER TABLE testString ADD INDEX idx_1(f1);
INSERT INTO testString(f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES('1','abcde','ab123','1adf','hello gt-checksum','aa','hello gt-checksum','d','aa,bb');

-- 测试二进制字符串类型
DROP TABLE IF EXISTS testBin;
CREATE TABLE testBin(
    f1 BINARY,
    f2 BINARY(3),
    f3 VARBINARY(10),
    f4 TINYBLOB,
    f5 BLOB,
    f6 MEDIUMBLOB,
    f7 LONGBLOB
) CHARACTER SET 'utf8';
ALTER TABLE testBin ADD INDEX idx_1(f1);
INSERT INTO testBin(f1,f2,f3,f4,f5,f6,f7) VALUES('a','abc','ab','01010101','0x9023123123','hello gt-checksum','hello gt-checksum');

-- 索引列为null或为''的处理


-- 触发器的处理

-- 测试表及测试数据
DROP TABLE IF EXISTS account;
CREATE TABLE account (acct_num INT, amount DECIMAL(10,2));
INSERT INTO account VALUES(137,14.98),(141,1937.50),(97,-100.00);

-- 创建影子表
DROP TABLE IF EXISTS tmp_account;
CREATE TABLE tmp_account (acct_num INT, amount DECIMAL(10,2),sql_text VARCHAR(100));

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

-- 测试步骤
-- insert 测试
INSERT INTO account VALUES (150,33.32);
SELECT * FROM tmp_account WHERE acct_num=150;

-- update 测试
INSERT INTO account VALUES(200,13.23);
UPDATE account SET acct_num = 201 WHERE amount = 13.23;
SELECT * FROM tmp_account;

-- delete 测试
INSERT INTO account VALUES(300,14.23);
DELETE FROM account WHERE acct_num = 300;
SELECT * FROM tmp_account;

-- 分区
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

DROP TABLE IF EXISTS gtchecksum.CUSTOMER;
CREATE TABLE gtchecksum.CUSTOMER(
    CUSTOMER_ID INT NOT NULL PRIMARY KEY,
    FIRST_NAME  VARCHAR(30) NOT NULL,
    LAST_NAME   VARCHAR(30) NOT NULL,
    PHONE        VARCHAR(15) NOT NULL,
    EMAIL        VARCHAR(80),
    STATUS       CHAR(1)
)PARTITION BY RANGE (CUSTOMER_ID)(
 PARTITION CUS_PART1 VALUES LESS THAN (100000),
 PARTITION CUS_PART2 VALUES LESS THAN (200000)
);

DROP TABLE IF EXISTS gtchecksum.CUSTOMER1;
CREATE TABLE gtchecksum.CUSTOMER1(
    CUSTOMER_ID VARCHAR(30) NOT NULL,
    FIRST_NAME  VARCHAR(30) NOT NULL,
    LAST_NAME   VARCHAR(30) NOT NULL,
    PHONE       VARCHAR(15) NOT NULL,
    EMAIL       VARCHAR(80),
    STATUS      CHAR(1)
) PARTITION BY RANGE COLUMNS (CUSTOMER_ID)(
    PARTITION CUS_PART1 VALUES LESS THAN ('100000'),
    PARTITION CUS_PART2 VALUES LESS THAN ('200000')
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

-- 存储函数
DELIMITER ||
DROP FUNCTION IF EXISTS getAgeStr;
CREATE FUNCTION getAgeStr(age INT)
RETURNS VARCHAR(20)
DETERMINISTIC
NO SQL
BEGIN
	DECLARE results VARCHAR(20);
	IF age<=14 then
		set results = '儿童';
	ELSEIF age <=24 THEN
		set results = '青少年';
	ELSEIF age <=44 THEN
		set results = '青年';
	ELSEIF age <=59 THEN
		set results = '中年';
ELSE
		SET results = '老年';
END IF;
RETURN results;
END ||
DELIMITER ;

-- 触发器
DROP TABLE IF EXISTS test1;
CREATE TABLE test1(a1 INT);
DROP TABLE IF EXISTS test2;
CREATE TABLE test2(a2 INT);
DELIMITER ||
DROP TRIGGER IF EXISTS tri_test;
CREATE TRIGGER tri_test
 BEFORE INSERT ON test1 FOR EACH ROW BEGIN
  INSERT INTO test2 SET a2=NEW.a1;
END ||
DELIMITER ;

/*
    索引
 */
DROP TABLE IF EXISTS IndexT;
CREATE TABLE IndexT(
    `id` INT(11) NOT NULL,
    `tenantry_id` BIGINT(20) NOT NULL COMMENT '商品id',
    `code` VARCHAR(64) NOT NULL COMMENT '商品编码（货号）',
    `goods_name` VARCHAR(50) NOT NULL COMMENT '商品名称',
    `props_name` VARCHAR(100) NOT NULL COMMENT '商品名称描述字符串，格式：p1:v1;p2:v2，例如：品牌:盈讯;型号:F908',
    `price` DECIMAL(10,2) NOT NULL COMMENT '商品定价',
    `price_url` VARCHAR(1000) NOT NULL COMMENT '商品主图片地址',
    `create_time` DATETIME NOT NULL COMMENT '商品创建时间',
    `modify_time` DATETIME DEFAULT NULL COMMENT '商品最近修改时间',
    `deleted` TINYINT(1) NOT NULL DEFAULT '0' COMMENT '标记逻辑删除',
    PRIMARY KEY (`id`),
    KEY `idx_2` (`tenantry_id`,`code`),
    KEY `idx_3` (`code`,`tenantry_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='商品信息表';