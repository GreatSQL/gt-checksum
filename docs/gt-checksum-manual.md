# gt-checksum 手册

## 关于gt-checksum

**gt-checksum** 是GreatSQL社区开源的数据库校验及修复工具，支持MySQL、Oracle等主流数据库。

## 用法

- 命令行传参方式
```bash
$ gt-checksum -S srcDSN -D dstDSN -t TABLES
```

- 指定配置文件方式

```bash
$ gt-checksum -f ./gc.conf
```

## 数据库授权

运行 gt-checksum 工具前，建议创建相应的专属数据库账户，并至少授予以下几个权限。
  
- MySQL端

  1.创建专属账户

    执行下面的SQL命令，创建专属账户：

    ```sql
    CREATE USER 'checksum'@'%' IDENTIFIED WITH mysql_native_password BY 'Checksum@3306';
    ```

  2.全局权限

    如果是MySQL 8.0及以上版本，需授予 `REPLICATION CLIENT` 和 `SESSION_VARIABLES_ADMIN` 权限。如果MySQL 5.7级以下版本，则无需授予 `SESSION_VARIABLES_ADMIN` 权限。

  3.校验数据对象

    a.如果参数设置 `datafix=file`，则只需授予 `SELECT`权限；
    b.如果参数设置 `datafix=table`，则需要授予 `SELECT、INSERT、DELETE` 权限，如果还需要修复表结构不一致的情况，则需要 `ALTER` 权限。

  假设现在要对db1.t1做校验和修复，则可授权如下
  ```sql
  mysql> GRANT REPLICATION CLIENT, SESSION_VARIABLES_ADMIN ON *.* TO ...;
  mysql> GRANT SELECT, INSERT, DELETE ON db1.t1 TO ...;
  ```

- Oracle端

  1.全局权限

    需授予 `SELECT ANY DICTIONARY` 权限。

  2.校验数据对象

    a.如果参数设置 `datafix=file`，则只需授予 `SELECT ANY TABLE` 权限；
    b.如果参数设置 `datafix=table`，则需要授予 `SELECT ANY TABLE、INSERT ANY TABLE、DELETE ANY TABLE` 权限。

## 快速使用案例
### 快速使用案例：指定配置文件方式

提前修改配置文件 *gc.conf*，然后执行如下命令进行数据校验：

```bash
$ gt-checksum -f ./gc.conf
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

### 快速使用案例：命令行传参方式

通过命令行传参方式进行数据校验，执行以下命令，实现目标：只校验 *db1* 库下的所有表，不校验 *test* 库下的所有表，并且没有索引的表也要校验：

```bash
$ gt-checksum -S type=mysql,user=checksum,passwd=Checksum@3306,host=172.16.0.1,port=3306,charset=utf8mb4 -D type=mysql,user=checksum,passwd=Checksum@3306,host=172.16.0.2,port=3306,charset=utf8mb4 -t db1.* -it test.* -nit yes
```

## 运行参数详解

**gt-checksum** 支持命令行传参及指定配置文件两种方式运行，但不支持两种方式同时指定。

配置文件参数详解可参考模板文件 [gc.conf.example](./gc.conf.example)，在该模板文件中有各个参数的详细解释。

**gt-checksum** 命令行参数选项详细解释如下。

- `--config / -f`。类型：**string**，默认值：**空**。作用：指定配置文件名。

  使用案例：
  ```bash
  $ gt-checksum -f ./gc.conf
  ```

  还支持极简配置文件工作方式，即只需要最少的几个参数就能快速执行，例如：

  ```bash
  $ cat gc.conf-simple
  [DSNs]
  srcDSN = mysql|checksum:Checksum@3306@tcp(172.17.16.1:3306)/information_schema?charset=utf8mb4
  dstDSN = mysql|checksum:Checksum@3306@tcp(172.17.16.2:3306)/information_schema?charset=utf8mb4

  [Schema]
  tables = db1.t1
  ```
  **注意**：

  1. 极简配置文件工作方式下，配置文件名必须是 `gc.conf-simple`，其他名字无效。
  2. 配置文件中仅需指定源和目标端的DSN，以及要校验的表名即可。

- `--srcDSN / -S`。类型：**String**，默认值：**port=3306,charset=utf8mb4**。作用：定义数据校验源数据库的DSN。

  使用案例：

  ```bash
  $ gt-checksum -S type=mysql,user=checksum,passwd=Checksum@3306,host=172.17.140.47,port=3306,charset=utf8mb4
  ```
  目前DSN定义支持MySQL、Oracle两种数据库。

  MySQL数据库的连接串格式为：`mysql|usr:password@tcp(ip:port)/dbname?charset=xxx`。例如：`dstDSN = mysql|pcms:abc123@tcp(172.16.0.1:3306)/information_schema?charset=utf8mb4`。其中，`port`默认值是**3306**，`charset`默认值是**utf8mb4**。

  Oracle的连接串格式为：`oracle|user/password@ip:port/sid`。例如：`srcDSN=oracle|pcms/abc123@172.16.0.1:1521/helowin`。
    
- `--dstDSN / -D`。类型：**String**，默认值：**port=3306,charset=utf8mb4**。作用：定义数据校验目标数据库的DSN。

  使用案例：

  ```bash
  $ gt-checksum -D type=mysql,user=checksum,passwd=Checksum@3306,host=172.17.140.47,port=3306,charset=utf8mb4
  ```

  和参数 **srcDSN** 一样，只支持MySQL、Oracle两种数据库，字符串格式要求也一样。

- `--table / -t`。类型：**String**。默认值：**空**。作用：定义要执行数据校验的数据表对象列表，支持通配符 `"%"` 和 `"*"`。

  使用案例：

  ```bash
  $ gt-checksum -S srcDSN -D dstDSN -t db1.*
  ```

  数据表名支持的字符有：`[0-9 a-z! @ _ {} -]. [0-9 a-z! @ _ {} -]`，超出这些范围的数据表名将无法识别。

  下面是其他几个案例：
  - `*.*` 表示所有库表对象（如果是MySQL数据则不包含 `information_schema\mysql\performance_schema\sys`）。
  - `test.*` 表示test库下的所有表。
  - `test.t%` 表示test库下所有表名中包含字母"t"开头的表。
  - `db%.*` 表示所有库名中包含字母"db"开头的数据库中的所有表。
  - `%db.*` 表示所有库名中包含字母"db"结尾的数据库中的所有表。

  **注意**：如果已经设置为 `"*.*"` 规则，则不能再增加其他的规则。例如，当设置 `"*.*,pcms%.*"` 时会报告规则错误。如果 `--table` 和 `--ignore-tables` 参数设置为相同值的话也会报告规则错误。

- `--ignore-table / -it`。类型：**String**。默认值：**空**。作用：定义不要执行数据校验的数据表对象列表，支持通配符 `"%"` 和 `"*"`。

  使用案例：

  ```bash
  $ gt-checksum -S srcDSN -D dstDSN -t db1.* -it test.*
  ```

  本参数的用法和规则和上面 `--table` 参数一样。

- `--CheckNoIndexTable / -nit`。类型：**Bool**，可选值：**yes/no**，默认值：**no**。作用：设置是否检查没有索引的表。

  **注意**：当设置为yes时，会对没有索引的表也执行数据校验，这个校验过程可能会非常慢。

  使用案例：

  ```bash
  $ gt-checksum -S srcDSN -D dstDSN -t db1.* -nit yes
  ```

- `--lowerCase / -lc`。类型：**Bool**，可选值：**yes/no**，默认值：**no**。作用：设置表名大小写规则。

  当设置为 yes，则按照配置参数的大小写进行匹配；当设置为 no，则统一用大写表名进行匹配。

  使用案例：

  ```bash
  $ gt-checksum -S srcDSN -D dstDSN -t db1.* -lc no
  ```

- `--logFile / -lf`。类型：**String**，默认值：**./gt-checksum.log**。作用：设置日志文件名，可以指定为绝对路径或相对路径。

  使用案例：

  ```bash
  $ gt-checksum -S srcDSN -D dstDSN -t db1.* -lf ./gt-checksum.log
  ```

- `--logLevel, -ll`。类型：**Enum**，可选值：**[debug|info|warn|error]**，默认值：**info**。作用：设置日志等级。

  使用案例：

  ```bash
  $ gt-checksum -S srcDSN -D dstDSN -t db1.* -lf ./gt-checksum.log -ll info
  ```

- `--parallel-thds / -thds`。类型：**Int**，默认值：**5**。作用：设置数据校验并行线程数。

  **注意**：该参数值必须设置大于0才支持并行。并行线程数越高，数据校验速度越快，但系统负载也会越高，网络连接通信也可能会成为瓶颈。

  使用案例：
  
  ```bash
  $ gt-checksum -S srcDSN -D dstDSN -t db1.* -thds 5
  ```

- `--chunkSize / -cs`。类型：**Int**，默认值：**1000**。作用：设置每次检索多少条数据进行校验。

  **提醒**：参数值设置范围建议：1000 - 5000。该参数值设置太大时有可能会造成SQL查询效率反倒下降的情况发生，一般建议设置不超过5000。

  使用案例：
    
  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -sicr 1000
  ```

- `--queue-size / -qs`。类型：**Int**，默认值：**100**。作用：设置数据校验队列深度。

  **提醒**：数据校验队列深度值设置越大，校验的速度也会越快，但需要消耗的内存会越高，注意避免服务器内存消耗过大。

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -qs 100
  ```

- `--checkMode / -cm`。类型：**Enum**，可选值：**[count|rows|sample]**，默认值：**rows**。作用：设置数据校验模式。

  - **count**：表示只校验源、目标表的数据量。
  - **rows**：表示对源、目标数据进行逐行校验。
  - **sample**：表示只进行抽样数据校验，配合参数`--ratio`设置采样率。

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -cm rows
  ```

- `--ratio / -r`。类型：**Int**，默认值：**10**。作用：设置数据采样率。

  当参数设置 `--checkMode = sample` 时，本参数有效。设置范围 **[1-100]**，表示相应的百分比。

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -cm sample -r 10
  ```

- `--checkObject / -co`。类型：**Enum**，可选值：**[data|struct|index|partitions|foreign|trigger|func|proc]**，默认值：**data**。作用：设置数据校验对象。

  几个可选参数值分别表示：**行数据|表结构|索引|分区|外键|触发器|存储函数|存储过程**。

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -co data
  ```

- `--ScheckFixRule / -scfr`。类型：**Enum**，可选值：**[src|dst]**，默认值：**src**。作用：设置在表结构校验时，数据修复时的对准原则，选择源端（**src**）或目标端（**dst**）作为修复校对依据。

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -sfr=src
  ```

- `--ScheckOrder / -sco`，类型：**Bool**，可选值：**yes/no**，默认值：**no**。作用：设置表结构数据校验时，是否要检查数据列的顺序。
  

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -sco=yes
  ```

- `--ScheckMod / -scm`，类型：**Enum**，可选值：**[loose|strict]**，默认值：**strict**。作用：设置表结构校验时采用严格还是宽松模式。

  - **loose**：宽松模式，只匹配数据列名。
  - **strict**：严格模式，严格匹配数据列的属性，列的属性包括 **数据类型、是否允许为null、默认值、字符集、校验集、comment** 等。

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -scm=strict
  ```

- `--datafix / -df`，类型：**Enum**，可选值：**[file|table]**，默认值：**file**。作用：设置数据修复方式。

  - **file**：生成数据修复SQL文件，不执行修复，后续手动执行修复。
  - **table**：直接在线修复数据。

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -df file
  ```

- `--fixFileName / -ffn`，类型：**String**。默认值：**./gt-checksum-DataFix.sql**。作用：设置生成数据修复SQL文件的文件名，可以指定为绝对路径或相对路径。

  - 当参数设置 `--datafix=file` 时，设置生成的SQL文件名，可以指定为绝对路径或相对路径。
  - 当参数设置 `--datafix=table` 时，无需设置本参数。

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -df file -ffn ./gt-checksumDataFix.sql
  ```

- `--fixTrxNum / -ftn`，类型：**Int**。默认值：**100**。作用：设置在一个数据修复事务中最多多少条SQL。

  通常建议批量执行数据修复事务。本参数用于设置在一个事务中最多运行多少条SQL，或者生成数据修复的SQL文件时，在生成的SQL文件中显式添加 `BEGIN ... COMMIT` 事务起止符中间的SQL语句数量。

  使用案例：

  ```bash
  $ gt-checksum -S DSN -D DSN -t db1.* -ftn=100
  ```

- `--help / -h`。作用：查看帮助内容。

- `--version / -v`。作用：打印版本号。

## 下载二进制包

点击 [下载链接](https://gitee.com/GreatSQL/gt-checksum/releases) 下载预编译好的二进制文件包，已经在 Ubuntu、CentOS、RHEL 等多个系统环境下测试通过。

## 下载配置Oracle驱动程序

如果需要校验Oracle数据库，则还需要先下载Oracle数据库相应版本的驱动程序。例如：待校验的数据库为Oracle 11-2，则要下载Oracle 11-2的驱动程序，并使之生效，否则连接Oracle会报错。

### 下载Oracle Instant Client
从 [https://www.oracle.com/database/technologies/instant-client/downloads.html](https://www.oracle.com/database/technologies/instant-client/downloads.html) 下载免费的Basic或Basic Light软件包。

- oracle basic client, instantclient-basic-linux.x64-11.2.0.4.0.zip

- oracle sqlplus, instantclient-sqlplus-linux.x64-11.2.0.4.0.zip

- oracle sdk, instantclient-sdk-linux.x64-11.2.0.4.0.zip

### 配置oracle client并生效
```bash
$ unzip instantclient-basic-linux.x64-11.2.0.4.0.zip
$ unzip instantclient-sqlplus-linux.x64-11.2.0.4.0.zip
$ unzip instantclient-sdk-linux.x64-11.2.0.4.0.zip
$ mv instantclient_11_2 /usr/local
$ echo "export LD_LIBRARY_PATH=/usr/local/instantclient_11_2:$LD_LIBRARY_PATH" >> /etc/profile
$ source /etc/profile
```

## 源码编译
**gt-checksum** 工具采用Go语言开发，您可以下载源码编译生成二进制文件。

编译环境要求使用golang 1.17及以上版本，请先行配置好Go编译环境。

请参考下面方法下载源码并进行编译：
```bash
$ git clone https://gitee.com/GreatSQL/gt-checksum.git
$ cd gt-checksum
$ go build -o gt-checksum gt-checksum.go
```

编译完成后，将编译好的二进制文件拷贝到系统PATH路径下，即可使用：
```bash
$ chmod +x gt-checksum
$ mv gt-checksum /usr/local/bin
```

## 已知缺陷
截止最新的1.2.1版本中，当数据表没有显式主键，且表中有多行数据是重复的，可能会导致校验结果不准确。

源端有个表t1，表结构及数据如下：

```sql
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

```sql
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

目标端中的t1表只有8行数据，如果除去重复数据，两个表是一致的，这会导致校验的结果显示为一致。

```
...
** gt-checksum Overview of results **
Check time:  0.30s (Seconds)
Schema  Table   IndexCol        checkMod        Rows    Differences     Datafix
t1      T1      id,code         rows            10,8    no              file
```
这个问题我们会在未来的版本中尽快修复。

## BUGS
可以 [提交issue](https://gitee.com/GreatSQL/gt-checksum/issues) 查看或提交 gt-checksum 相关bug。
