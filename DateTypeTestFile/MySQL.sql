create database pcms;
use pcms;
#测试数值类型
create table testInt(
    f1 TINYINT,
    f2 SMALLINT,
    f3 MEDIUMINT,
    f4 INT,
    f5 INT(5) ZEROFILL,
    f6 INT UNSIGNED,
    f7 BIGINT
) CHARACTER SET 'utf8';
alter table testint add index idx_1(f1);
insert into testInt(f1,f2,f3,f4,f5,f6,f7) values(1,2,3,4,5,6,7);

create table  testFlod(
  f1 FLOAT,
  f2 FLOAT(5,2),
  f3 DOUBLE,
  f4 DOUBLE(5,3)
) CHARACTER SET 'utf8';
alter table testflod add index idx_1(f1);
insert into testFlod(f1,f2,f3,f4) values(123.45,123.45,123.45,12.456);

#测试二进制类型
create table testBit(
    f1 BIT,
    f2 BIT(5),
    F3 bit(64)
);
alter table testbit add index idx_1(f1);
insert into testBit values(1,31,65);
select *  from testBit;  #from bin,oct,hex bin转换为二进制，oct8进制，hex16进制
#测试时间类型
create table testTime(
     f1 YEAR,
     f2 YEAR(4),
     f3 date,
     f4 time,
     f5 datetime,
     f6 timestamp
)CHARACTER SET 'utf8';
alter table testtime add index idx_1(f1);
insert into testTime(f1,f2,f3,f4,f5,f6) values('2022',2022,'2022-07-12','2 12:30:29','2022-07-12 14:53:00','2022-07-12 14:54:00');

#测试字符串类型
create table testString(
   f1 char,
   f2 char(5),
   f3 varchar(10),
   f4 tinytext,
   f5 text,
   f6 mediumtext,
   f7 longtext,
   f8 enum('a','b','c','d'),
   f9 set('aa','bb','cc','dd')
)CHARACTER SET 'utf8';
alter table teststring add index idx_1(f1);
insert into testString(f1,f2,f3,f4,f5,f6,f7,f8,f9) values('1','abcde','ab123','1adf','aaadfaewrwer','aa','aasdfasdfafdafasdfasf','d','aa,bb');

#测试二进制字符串类型
create table testBin(
    f1 binary,
    f2 binary(3),
    f3 varbinary(10),
    f4 tinyblob,
    f5 blob,
    f6 mediumblob,
    f7 longblob
)character set 'utf8';
alter table testbin add index idx_1(f1);
insert into testBin(f1,f2,f3,f4,f5,f6,f7) values('a','abc','ab','01010101','0x9023123123','adfasdfasdfasdfasdf','aasdfasdfasdfasdfasf');

#索引列为null或为''的处理


#触发器的处理

//测试表及测试数据
CREATE TABLE account (acct_num INT, amount DECIMAL(10,2));
INSERT INTO account VALUES(137,14.98),(141,1937.50),(97,-100.00);

//创建影子表
CREATE TABLE tmp_account (acct_num INT, amount DECIMAL(10,2),sql_text varchar(100));

//监控insert
DELIMITER ||
create trigger accountInsert BEFORE insert
    on xxx for each row
BEGIN
    INSERT INTO tmp_account values(new.acct_num,new.amount,"insert");
end ||
delimiter;

//监控delete
DELIMITER ||
create trigger accountDelete BEFORE delete
    on xxx for each row
BEGIN
    insert into tmp_account values(old.acct_num,old.amount,"delete")
end ||
delimiter;

//监控update
DELIMITER ||
create trigger accountUpdate BEFORE update
    on xxx for each row
BEGIN
    insert into tmp_account values(old.acct_num,old.amount,"update_delete")
        insert into tmp_account values(new.acct_num,new.account,"update_insert")
end ||
delimiter;


//测试步骤
//insert 测试
insert into account values (150,33.32);
select * from tmp_account where acct_num=150;

//update 测试
insert into account values(200,13.23);
update account set acct_num = 201 where amount = 13.23；
select * from tmp_account

//delete 测试
insert into account values(300,14.23);
delete from account where acct_num = 300;
select * from tmp_account






