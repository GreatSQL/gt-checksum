//分区
CREATE TABLE range_Partition_Table(
    range_key_column DATE,
    NAME VARCHAR2(20),
    ID integer
) PARTITION BY RANGE(range_key_column)(
    PARTITION PART_202007 VALUES LESS THAN (TO_DATE('2020-07-1 00:00:00','yyyy-mm-dd hh24:mi:ss')),
    PARTITION PART_202008 VALUES LESS THAN (TO_DATE('2020-08-1 00:00:00','yyyy-mm-dd hh24:mi:ss')),
    PARTITION PART_202009 VALUES LESS THAN (TO_DATE('2020-09-1 00:00:00','yyyy-mm-dd hh24:mi:ss'))
);

CREATE TABLE "PCMS"."CUSTOMER"(
    CUSTOMER_ID NUMBER NOT NULL PRIMARY KEY,
    FIRST_NAME  VARCHAR2(30) NOT NULL,
    LAST_NAME   VARCHAR2(30) NOT NULL,
    PHONE        VARCHAR2(15) NOT NULL,
    EMAIL        VARCHAR2(80),
    STATUS       CHAR(1)
)PARTITION BY RANGE ("CUSTOMER_ID")(
 PARTITION CUS_PART1 VALUES LESS THAN (100000),
 PARTITION CUS_PART2 VALUES LESS THAN (200000)
)

CREATE TABLE list_Partition_Table(
    NAME VARCHAR2(10),
    DATA VARCHAR2(20)
)PARTITION BY LIST(NAME)(
    PARTITION PART_01 VALUES('ME','PE','QC','RD'),
    PARTITION PART_02 VALUES('SMT','SALE')
);

CREATE TABLE hash_Partition_Table(
    hash_key_column VARCHAR2(30),
    DATA VARCHAR2(20)
) PARTITION BY HASH(hash_key_column)(
    PARTITION PART_0001,
    PARTITION PART_0002,
    PARTITION PART_0003,
    PARTITION PART_0004,
    PARTITION PART_0005
);

CREATE TABLE range_hash_Partition_Table(
    range_column_key DATE,
    hash_column_key INT,
    DATA VARCHAR2(20)
) PARTITION BY RANGE(range_column_key)
    SUBPARTITION BY HASH(hash_column_key) SUBPARTITIONS 2
    (
   PARTITION PART_202008 VALUES LESS THAN (TO_DATE('2020-08-01','yyyy-mm-dd'))(
      SUBPARTITION SUB_1,
      SUBPARTITION SUB_2,
      SUBPARTITION SUB_3
   ),
   PARTITION PART_202009 VALUES LESS THAN (TO_DATE('2020-09-01','yyyy-mm-dd'))(
      SUBPARTITION SUB_4,
      SUBPARTITION SUB_5
   )
);


//外键
CREATE TABLE "PCMS"."tb_dept1" (
    "ID" NUMBER(11) NOT NULL,
    "NAME" VARCHAR2(22) NOT NULL,
    "LOCATION" VARCHAR2(50),
    PRIMARY KEY ("ID")
)
DROP TABLE "PCMS"."tb_emp6";
CREATE TABLE "PCMS"."tb_emp6" (
    "id" NUMBER(11,0) NOT NULL,
    "name" VARCHAR2(25 BYTE),
    "deptId" NUMBER(11,0),
    "salary" FLOAT(126)
)
ALTER TABLE "PCMS"."tb_emp6" ADD CONSTRAINT "SYS_C0011130" PRIMARY KEY ("id");
ALTER TABLE "PCMS"."tb_emp6" ADD CONSTRAINT "SYS_C0011129" CHECK ("id" IS NOT NULL) NOT DEFERRABLE INITIALLY IMMEDIATE NORELY VALIDATE;
ALTER TABLE "PCMS"."tb_emp6" ADD CONSTRAINT "fk_emp_dept1" FOREIGN KEY ("deptId") REFERENCES "PCMS"."tb_dept1" ("ID") NOT DEFERRABLE INITIALLY IMMEDIATE NORELY VALIDATE;

//存储函数
CREATE OR REPLACE FUNCTION FUN_getAgeStr(age int)
RETURN varchar2 IS
results varchar2(20);
BEGIN
 IF age<16 then
  results := '小屁孩';
 ELSIF age <22 THEN
  results := '小鲜肉';
 ELSIF age <30 THEN
  results := '小青年';
ELSE
  results := '大爷';
END IF;
RETURN results;
end;

//存储过程

CREATE TABLE "PCMS"."info" (
                               "ID" NUMBER NOT NULL,
                               "AGE" NUMBER NOT NULL,
                               "ADDRESS" VARCHAR2(20) NOT NULL,
                               "SALARY" NUMBER(10,2) NOT NULL,
                               PRIMARY KEY ("ID")
)
INSERT INTO "info"(ID,NAME,AGE,ADDRESS,SALARY) VALUES(1,'ZHANG',32,'Beijing',2000.00);
INSERT INTO "info"(ID,NAME,AGE,ADDRESS,SALARY) VALUES(2,'LI',25,'Shanghai',1500.00);
INSERT INTO "info"(ID,NAME,AGE,ADDRESS,SALARY) VALUES(3,'PENG',23,'Hangzhou',2000.00);
INSERT INTO "info"(ID,NAME,AGE,ADDRESS,SALARY) VALUES(4,'LIN',25,'Henan',6500.00);
INSERT INTO "info"(ID,NAME,AGE,ADDRESS,SALARY) VALUES(5,'WANG',27,'Hunan',8500.00);
INSERT INTO "info"(ID,NAME,AGE,ADDRESS,SALARY) VALUES(6,'WANG',22,'Hunan',4500.00);
INSERT INTO "info"(ID,NAME,AGE,ADDRESS,SALARY) VALUES(7,'GAO',24,'Hebei',10000.00);


CREATE OR REPLACE procedure countproc(sid IN INT,num OUT INT) is
begin
select count(*) into num from PCMS."info" where salary > 5000;
end;

//触发器
CREATE TABLE "test1"(a1 NUMBER);
CREATE TABLE "test2"(a2 NUMBER);
CREATE OR REPLACE TRIGGER tri_test
  BEFORE INSERT ON "test1"
FOR EACH ROW
BEGIN
INSERT INTO "test2"(a2) values(:NEW.a1);
commit;
END;
