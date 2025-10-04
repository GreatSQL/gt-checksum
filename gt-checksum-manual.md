# gt-checksum 手册

## 关于gt-checksum

**gt-checksum** 是GreatSQL社区开源的数据库校验及修复工具，支持MySQL、Oracle等主流数据库。

## 用法

指定完整配置文件方式运行

```bash
$ gt-checksum -c ./gc.conf
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

    如果是MySQL 8.0及以上版本，需授予 `REPLICATION CLIENT`, `SESSION_VARIABLES_ADMIN`, `SHOW_ROUTINE`, `TRIGGER` 权限。如果MySQL 5.7级以下版本，则无需授予 `SESSION_VARIABLES_ADMIN` 权限。

  3.校验数据对象

    a.如果参数设置 `datafix=file`，则只需授予 `SELECT`权限（生成修复SQL文件后，由管理员手动执行完成修复）；
    b.如果参数设置 `datafix=table`，则需要授予 `ALTER, SELECT、INSERT、DELETE` 权限。

  假设现在要对db1.t1做校验和修复，则可授权如下
  ```sql
  mysql> GRANT REPLICATION CLIENT, SESSION_VARIABLES_ADMIN ON *.* TO 'checksum'@'%';
  mysql> GRANT SELECT, INSERT, DELETE ON db1.t1 TO 'checksum'@'%';
  ```

  如果还要让执行校验的账户同时具备修复建表DDL、存储程序、触发器等数据对象的权限，则还需要更多授权（如 `SET_USER_ID,SHOW_ROUTINE,SYSTEM_USER,SYSTEM_VARIABLES_ADMIN` 等），整体授权如下例所示：
  ```sql
  mysql> GRANT REPLICATION CLIENT,SESSION_VARIABLES_ADMIN,SET_USER_ID,SHOW_ROUTINE,SYSTEM_USER,SYSTEM_VARIABLES_ADMIN ON *.* TO 'checksum'@'%';
  mysql> GRANT GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE ROUTINE, ALTER ROUTINE, TRIGGER ON test.* TO 'checksum'@'%';
  ```
  有时候，在创建Function时，还需要修改`log_bin_trust_function_creators`参数，否则会报错。此时还需要授予`SUPER`权限才行。

- Oracle端

  1.全局权限

    需授予 `SELECT ANY DICTIONARY` 权限。

  2.校验数据对象

    a.如果参数设置 `datafix=file`，则只需授予 `SELECT ANY TABLE` 权限；
    b.如果参数设置 `datafix=table`，则需要授予 `SELECT ANY TABLE、INSERT ANY TABLE、DELETE ANY TABLE` 权限。

## 快速使用案例

拷贝或重命名模板文件*gc-sample.conf*为*gc.conf*，主要修改`srcDSN`,`dstDSN`,`tables`,`ignoreTables`等几个参数后，执行如下命令进行数据校验：

```bash
$ gt-checksum -f ./gc.conf

Initializing gt-checksum
Reading configuration files
Opening log files
Checking configuration options
gt-checksum: Starting table checks
gt-checksum: Collecting table column information
gt-checksum: Collecting table index information
gt-checksum: Establishing database connections
gt-checksum: Generating data checksum plan

gt-checksum: Starting index checksum for table sbtest.sbtest2
gt-checksum: Table sbtest.sbtest2 checksum completed

Checksum Results Overview
Schema  Table   IndexColumn     CheckObject     Rows            Diffs   Datafix
sbtest  sbtest2 id              data            4999,4999       yes     file

Performance Metrics:
  Initialization: 0.00s
  Metadata collection: 0.00s
  Connection setup: 0.02s
  Data checksum: 0.06s
  Additional operations: 0.02s
  Miscellaneous: 0.01s
Total execution time: 0.11s
```

如果参数 `tables` 设置了映射规则，例如 `tables=db1.*:db2.*,sbtest.sbtest2`，则校验结果如下：

```bash
... 此处忽略前面的内容
...
Checksum Results Overview
Schema  Table                           IndexColumn     CheckObject     Rows            Diffs   Datafix Mapping
db1     test2                           NULL            data            0,0             no      file    Schema: db1:db2
db1     indext                          id              data            0,0             no      file    Schema: db1:db2
db1     tb_emp6                         id              data            0,0             no      file    Schema: db1:db2
sbtest  sbtest2                         id              data            4999,4999       yes     file    -
db1     testbin                         NULL            data            1,1             no      file    Schema: db1:db2
```

输出结果中，除了 **sbtest.sbtest2** 这个表所在行中 **Mapping** 列的值为 **-** 外，其他表的 **Mapping** 列的值都为 **Schema: db1:db2**，表示该表在源端和目标端的映射关系为 **db1.test2** 和 **db2.test2**。

如果参数 `checkObject` 设置为 **routine** 或 **trigger**，则只能判断是否不一致，但无法生成fixSQL或直接完成修复，例如：

```bash
...
Checksum Results Overview
Schema  RoutineName     CheckObject     DIFFS   Datafix
sbtest  MYADD           Procedure       yes     no
sbtest  P1              Procedure       no      no
sbtest  GETAGESTR       Function        yes     no
sbtest  F1              Function        no      no
...
```

虽然在 DIFFS 列中提示部分存储函数存在差异，但却都无法生成修复SQL，需要DBA介入判断后进行修复。

## 配置参数详解

**gt-checksum** 支持命令行传参及指定配置文件两种方式运行，但不支持两种方式同时指定。

配置文件中所有参数的详解可参考模板文件 [gc-sample.conf](./gc-sample.conf)。

**gt-checksum** 命令行参数选项详细解释如下。

- `-c / -f`。类型：**string**，默认值：**空**。作用：指定配置文件名。

  使用案例：
  ```bash
  $ gt-checksum -c ./gc.conf
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

> 我们提供下载的二进制包中已包含 instantclient_11_2.tar.xz 压缩包，下载后解开即可直接使用，无需再次下载。

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

截止最新的v1.2.3版本，已知存在以下几个问题。

- 不支持对非InnoDB引擎表的数据校验。

- 切换到"partitions|foreign|trigger|routine"等几个校验模式时，当校验结果不一致时，无法生成相应的修复SQL，即便设置`datafiex=table`也无法直接修复，需要DBA介入判断后手动修复。

- 当数据表没有显式主键，且表中有多行数据是重复的，可能会导致校验结果不准确。

源端有个表t1，表结构及数据如下：

```sql
mysql> SHOW CREATE TABLE t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` float(10,2) DEFAULT NULL,
  `code` varchar(10) DEFAULT NULL,
  KEY `idx_1` (`id`,`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

mysql> SELECT * FROM t1;
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
mysql> SELECT * FROM t1;
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
Check time:  0.30s
Schema  Table   IndexColumn      Rows    Diffs     Datafix
t1      T1      id,code          10,8    no        file
```
这个问题我们会在未来的版本中尽快修复。

## 问题反馈

可以 [提交issue](https://gitee.com/GreatSQL/gt-checksum/issues) 查看或提交 gt-checksum 相关bug。
