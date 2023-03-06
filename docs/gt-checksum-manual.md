# gt-checksum
---

## 关于gt-checksum
---
gt-checksum - A opensource table and data checksum tool by GreatSQL

## 用法
---
Usage:

```
gt-checksum --srcDSN DSN --dstDSN DSN --tables TABLES
```

or

```
gt-checksum --config=./gc.conf
```

### 数据库授权
想要运行gt-checksum工具，需要至少授予以下几个权限：
- 在MySQL端
	- 1.全局权限
		- a.`REPLICATION CLIENT`
		b.`SESSION_VARIABLES_ADMIN`，如果是MySQL 8.0版本的话，MySQL 5.7版本不做这个要求
  - 2.校验数据对象
		- a.如果`datafix=file`，则只需要`SELECT`权限
		- b.如果`datafix=table`，则需要`SELECT、INSERT、DELETE`权限

假设现在要对db1.t1做校验和修复，则可授权如下

```
mysql> GRANT REPLICATION CLIENT, SESSION_VARIABLES_ADMIN ON *.* to ...;
mysql> GRANT SELECT, INSERT, DELETE ON db1.t1 to ...;
```
- 在Oracle端
	- 1.全局权限
		- a.`SELECT ANY DICTIONARY`
  - 2.校验数据对象
		- a.如果`datafix=file`，则只需要`SELECT ANY TABLE`权限
		- b.如果`datafix=table`，则需要`SELECT ANY TABLE、INSERT ANY TABLE、DELETE ANY TABLE`权限


### 快速使用案例1
---
指定配置文件，开始执行数据校验，示例：
```shell
shell> ./gt-checksum -f ./gc.conf
-- gt-checksum init configuration files --
-- gt-checksum init log files --
-- gt-checksum init check parameter --
-- gt-checksum init check table name --
-- gt-checksum init check table column --
-- gt-checksum init check table index column --
-- gt-checksum init source and dest transaction snapshoot conn pool --
-- gt-checksum init cehck table query plan and check data --
begin checkSum index table db1.t1
[█████████████████████████████████████████████████████████████████████████████████████████████████████████████████]113%  task:     678/600
table db1.t1 checksum complete

** gt-checksum Overview of results **
Check time:  73.81s (Seconds)
Schema  Table                   IndexCol                                checkMod        Rows            Differences     Datafix
db1     t1                      ol_w_id,ol_d_id,ol_o_id,ol_number       rows            5995934,5995918 yes             file
```

### 快速使用案例2
---
设定只校验db1库下的所有表，不校验test库下的所有表，并设置没有索引的表也要校验
```
./gt-checksum -S type=mysql,user=root,passwd=abc123,host=172.16.0.1,port=3306,charset=utf8 -D type=mysql,user=root,passwd=abc123,host=172.16.0.2,port=3306,charset=utf8 -t db1.* -it test.* -nit yes
```

## gt-checksum特性
---
MySQL DBA最常用的数据校验&修复工具应该是Percona Toolkit中的pt-table-checksum和pt-table-sync这两个工具，不过这两个工具并不支持MySQL MGR架构，以及国内常见的上云下云业务场景，还有MySQL、Oracle间的异构数据库等多种场景。

GreatSQL开源的gt-checksum工具可以满足上述多种业务需求场景，解决这些痛点。

gt-checksum工具支持以下几种常见业务需求场景：
1. **MySQL主从复制**：主从复制中断后较长时间才发现，且主从间差异的数据量太多，这时候通常基本上只能重建复制从库，如果利用pt-table-checksum先校验主从数据一致性后，再利用pt-table-sync工具修复差异数据，这个过程要特别久，时间代价太大。
2. **MySQL MGR组复制**：MySQL MGR因故崩溃整个集群报错退出，或某个节点异常退出，在恢复MGR集群时一般要面临着先检查各节点间数据一致性的需求，这时通常为了省事会选择其中一个节点作为主节点，其余从节点直接复制数据重建，这个过程要特别久，时间代价大。
3. **上云下云业务场景**：目前上云下云的业务需求很多，在这个过程中要进行大量的数据迁移及校验工作，如果出现字符集改变导致特殊数据出现乱码或其他的情况，如果数据迁移工具在迁移过程中出现bug或者数据异常而又迁移成功，此时都需要在迁移结束后进行一次数据校验才放心。
4. **异构迁移场景**：有时我们会遇到异构数据迁移场景，例如从Oracle迁移到MySQL，通常存在字符集不同，以及数据类型不同等情况，也需要在迁移结束后进行一次数据校验才放心。
5. **定期校验场景**：作为DBA在维护高可用架构中为了保证主节点出现异常后能够快速放心切换，就需要保证各节点间的数据一致性，需要定期执行数据校验工作。

以上这些场景，都可以利用gt-chcksum工具来满足。
 

## 参数选项详解
---
gt-checksum支持命令行传参，或者指定配置文件两种方式运行，但不支持两种方式同时指定。

配置文件可参考(这个模板)[x]，模板中包含相关参数的详细解释。

gt-checksum命令行参数选项详细解释如下：

- --config / -f 
  Type: string

  指定配置文件名，例如：

```shell
shell> ./gt-checksum -f ./gc.conf
shell> ./gt-checksum --config ./gc.conf
```
gt-checksum支持极简配置文件工作方式，即只需要最少的几个参数就能工作，例如：
```shell
#
shell> cat gc.conf-simple
[DSNs]
srcDSN = mysql|pcms:abc123@tcp(172.17.16.1:3306)/information_schema?charset=utf8
dstDSN = mysql|pcms:abc123@tcp(172.17.16.2:3306)/information_schema?charset=utf8

[Schema]
tables = db1.t1
```
**注意**：

1. 极简配置文件名必须是 `gc.conf-simple`。
2. 配置文件中仅需指定源和目标端的DSN，以及要校验的表名即可。

- --srcDSN / -S
  Type: String. Default: port=3306,charset=utf8mb4.

  定义数据校验源数据库的DSN，例如：
```
  -S type=mysql,user=root,passwd=abc123,host=172.17.140.47,port=3306,charset=utf8mb4
```
  当前DSN定义支持MySQL、Oracle两种数据库。  

  Oracle的连接串格式为：`oracle|user/password@ip:port/sid`
  例如：`srcDSN = oracle|pcms/abc123@172.16.0.1:1521/helowin`

  MySQL的连接串格式为：`mysql|usr:password@tcp(ip:port)/dbname?charset=xxx`
  例如：`dstDSN = mysql|pcms:abc123@tcp(172.16.0.1:3306)/information_schema?charset=utf8`

  注：port默认值是3306，charset默认值是utf8mb4。
    
- --dstDSN / -D
  Type: String. Default: port=3306,charset=utf8mb4.

  定义数据校验目标数据库的DSN，例如：

```
-D type=mysql,user=root,passwd=abc123,host=172.17.140.47,port=3306,charset=utf8mb4
```
  和srcDSN一样，也支持MySQL、Oracle两种数据库，DSN字符串格式同srcDSN。

  注：port默认值是3306，charset默认值是utf8mb4。

- --table / -t
  Type: String. Default: nil.

  定义要执行数据校验的数据表对象列表，支持通配符"%"和"*"。

  表名中支持的字符有：[0-9 a-z! @ _ {} -]. [0-9 a-z! @ _ {} -]，超出这些范围的表名将无法识别。

  下面是几个案例：
  - *.* 表示所有库表对象（MySQL不包含 information_schema\mysql\performance_schema\sys）
  - test.* 表示test库下的所有表
  - test.t% 表示test库下所有表名中包含字母"t"开头的表
  - db%.* 表示所有库名中包含字母"db"开头的数据库中的所有表
  - %db.* 表示所有库名中包含字母"db"结尾的数据库中的所有表

  如果已经设置为 "*.*"，则不能再增加其他的规则，例如：设置 "*.*,pcms%.*" 则会报告规则错误。 如果 table 和 ignore-tables 设置的值相同的话也会报告规则错误。

  案例：
```shell
shell> gt-checksum -S srcDSN -D dstDSN -t db1.*
```

- --ignore-table / -it
  Type: String. Default: nil.

  定义不要执行数据校验的数据表对象列表，支持通配符"%"和"*"。

  表名中支持的字符有：[0-9 a-z! @ _ {} -]. [0-9 a-z! @ _ {} -]，超出这些范围的表名将无法识别。

  具体用法参考上面 --table 选项中的案例。

  案例：
```shell
shell> gt-checksum -S srcDSN -D dstDSN -t db1.* -it test.*
```

- --noIndexTable / -nit
  Type: Bool, yes/no. Default: no.

  设置是否检查没有索引的表，可设置为：yes/no，默认值为：no。

  当设置为yes时，会对没有索引的表也执行数据校验，这个校验过程可能会非常慢。

  案例：
```shell
shell> gt-checksum -S srcDSN -D dstDSN -t db1.* -nit yes
```

- --lowerCase / -lc
  Type: Bool, yes/no. Default: no.

  设置是否忽略表名大小写，可统一使用小写表名，设置为：yes/no，默认值为：no。

  案例：
```shell
shell> gt-checksum -S srcDSN -D dstDSN -t db1.* -lc no
```

- --logFile / -lf
  Type: String. Default: ./gt-checksum.log.

  设置日志文件名，可以指定为绝对路径或相对路径。

./gt-checksum -S DSN -D DSN -lf  gt-checksum.log

  案例：
```shell
shell> gt-checksum -S srcDSN -D dstDSN -t db1.* -lf ./gt-checksum.log
```

- --logLevel, -ll
  Type: String, debug/info/warn/error. Default: info.  

  设置日志等级，支持 debug/info/warn/error 几个等级，默认值为：info。

  案例：
```shell
shell> gt-checksum -S srcDSN -D dstDSN -t db1.* -lf ./gt-checksum.log -ll info
```

- --parallel-thds / -thds
  Type: Int. Default: 5.

  设置数据校验并行线程数。该值必须设置大于0，并行线程数越高，数据校验速度越快，系统负载也会越高，网络连接通信也可能会成为瓶颈。

  案例：
```shell
shell> gt-checksum -S srcDSN -D dstDSN -t db1.* -thds 5
```

- --singleIndexChanRowCount / -sicr
  Type: Int. Default: 1000.

  设置单列索引每次检索多少条数据进行校验，默认值：1000，建议范围：1000 - 5000。

  注：该值设置太大时有可能会造成SQL查询效率反倒下降的情况发生，一般建议设置不超过5000。

  案例：
```shell
./gt-checksum -S DSN -D DSN -t db1.* -sicr 1000
```

- --jointIndexChanRowCount / -jicr
  Type: Int. Default: 1000.

  设置多列索引每次检索多少条数据进行校验，默认值：1000，建议范围：1000 - 5000。

  注：该值设置太大时有可能会造成SQL查询效率反倒下降的情况发生，一般建议设置不超过5000。

  案例：
```shell
./gt-checksum -S DSN -D DSN -t db1.* -jicr 1000
```

- --queue-size / -qs
  Type: Int. Default: 100.
  
  设置数据校验队列深度，默认值：100。

  数据校验队列深度值设置越大，需要消耗的内存会越高，校验的速度也会越快。

  案例：
```shell
./gt-checksum -S DSN -D DSN -t db1.* -qs 100
```

- --checkMode / -cm
  Type: enum, count/rows/sample. Default: rows.

  设置数据校验模式，支持 count/rows/sample 三种模式，默认值为：rows

  count 表示只校验源、目标表的数据量

  rows 表示逐行校验源、目标数据

  sample 表示只进行抽样数据校验，配合参数ratio设置采样率

  案例：
```shell
./gt-checksum -S DSN -D DSN -t db1.* -cm rows
```

- --ratio / -r
  Type: Int. Default: 10.

  当 `checkMode = sample` 时，设置数据采样率，设置范围1-100，用百分比表示，1表示1%，100表示100%，默认值：10。

  案例：
```shell
./gt-checksum -S DSN -D DSN -t db1.* -cm sample -r 10
```

- --checkObject / -co
  Type: enum, data/struct/index/partitions/foreign/trigger/func/proc. Default: data.

  设置数据校验对象，支持 data/struct/index/partitions/foreign/trigger/func/proc，默认值为：data

  分别表示：行数据/表结构/索引/分区/外键/触发器/存储函数/存储过程。

  案例：
```shell
./gt-checksum -S DSN -D DSN -t db1.* -co data
```

- --datafix / -df
  Type: enum, table/file. Default: file.

  设置数据修复方式，支持 file/table 两种方式。file：生成数据修复SQL文件；table：直接在线修复数据。

  案例：
```shell
./gt-checksum -S DSN -D DSN -t db1.* -df file
or
./gt-checksum -S DSN -D DSN -t db1.* -df table
```

- --fixFileName / -ffn
  Type: String. Default: ./gt-checksum-DataFix.sql

  当 datafix = file 时，设置生成的SQL文件名，可以指定为绝对路径或相对路径。

  当 datafix = table 时，可以不用设置 fixFileName 参数。

./gt-checksum -S DSN -D DSN -ffn gt-checksum-DataFix.sql

  案例：
```shell
./gt-checksum -S DSN -D DSN -t db1.* -df file -ffn ./gt-checksumDataFix.sql
```
- --fixTrxNum / -ftn
  Type: Int. Default: 100.

  设置执行数据修复时一个事务中最多运行多少条SQL，或者生成数据修复的SQL文件时，显式在SQL文件中添加 begin + commit 事务起止符中间的SQL语句数量。

  案例：
```shell
./gt-checksum -S DSN -D DSN -t db1.* -ftn=100
```

- --help / -h
  查看帮助内容。

- --version /  -v
  打印版本号。

## 下载
---
可以 [这里](https://gitee.com/GreatSQL/gt-checksum/releases) 下载预编译好的二进制文件包，已经在Ubuntu、CentOS、RHEL等多个下测试通过。

如果需要校验Oracle数据库，则还需要先下载Oracle数据库相应版本的驱动程序，并配置驱动程序使之生效。例如：待校验的数据库为Oracle 11-2，则要下载Oracle 11-2的驱动程序，并使之生效，否则连接Oracle会报错。详细方法请见下方内容：[**下载配置Oracle驱动程序**](x) 。


## 下载配置Oracle驱动程序
---
如果需要校验Oracle数据库，则还需要先下载Oracle数据库相应版本的驱动程序。例如：待校验的数据库为Oracle 11-2，则要下载Oracle 11-2的驱动程序，并使之生效，否则连接Oracle会报错。

### 下载Oracle Instant Client
从 [https://www.oracle.com/database/technologies/instant-client/downloads.html](https://www.oracle.com/database/technologies/instant-client/downloads.html) 下载免费的Basic或Basic Light软件包。

- oracle basic client, instantclient-basic-linux.x64-11.2.0.4.0.zip

- oracle sqlplus, instantclient-sqlplus-linux.x64-11.2.0.4.0.zip

- oracle sdk, instantclient-sdk-linux.x64-11.2.0.4.0.zip

### 配置oracle client并生效
```shell
shell> unzip instantclient-basic-linux.x64-11.2.0.4.0.zip
shell> unzip instantclient-sqlplus-linux.x64-11.2.0.4.0.zip
shell> unzip instantclient-sdk-linux.x64-11.2.0.4.0.zip
shell> mv instantclient_11_2 /usr/local
shell> echo "export LD_LIBRARY_PATH=/usr/local/instantclient_11_2:$LD_LIBRARY_PATH" >> /etc/profile
shell> source /etc/profile
```

## 源码编译
gt-checksum工具采用GO语言开发，您可以自行编译生成二进制文件。

编译环境要求使用golang 1.17及以上版本。

请参考下面方法下载源码并进行编译：
```shell
shell> git clone https://gitee.com/GreatSQL/gt-checksum.git
shell> go build -o gt-checksum gt-checksum.go
shell> chmod +x gt-checksum
shell> mv gt-checksum /usr/local/bin
```

## 已知缺陷
截止最新的1.2.0版本中，当表中有多行数据是完全重复的话，可能会导致校验结果不准确。

源端有个表t1，表结构及数据如下：
```
mysql> show create table t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` float(10,2) DEFAULT NULL,
  `code` varchar(10) DEFAULT NULL,
  KEY `idx_1` (`id`,`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

mysql> select * from t1;
+-------+------+
| id    | code |
+-------+------+
|  1.01 | a    |
|  1.50 | b    |
|  2.30 | c    |
|  3.40 | d    |
|  4.30 | NULL |
|  4.30 | NULL |
|  4.30 | NULL |
|  4.30 |      |
|  4.30 | f    |
| 10.10 | e    |
+-------+------+
10 rows in set (0.00 sec)
```
**注意**：上述10行数据中，有3行数据是完全一致的。

目标端中同样也有t1表，表结构完全一样，但数据不一样：
```
mysql> select * from t1;
+-------+------+
| id    | code |
+-------+------+
|  1.01 | a    |
|  1.50 | b    |
|  2.30 | c    |
|  3.40 | d    |
|  4.30 | NULL |
|  4.30 |      |
|  4.30 | f    |
| 10.10 | e    |
+-------+------+
8 rows in set (0.00 sec)
```

可以看到，目标端中的t1表只有8行数据，如果除去重复数据，两个表是一致的，这也会导致校验的结果显示为一致。
```
...
** gt-checksum Overview of results **
Check time:  0.30s (Seconds)
Schema  Table   IndexCol        checkMod        Rows    Differences     Datafix
t1      T1      id,code         rows            10,8    no              file
```
这个问题我们会在未来某个版本中尽快修复。

## BUGS
---
可以 [戳此](https://gitee.com/GreatSQL/gt-checksum/issues) 查看 gt-checksum 相关bug列表。
