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
  mysql> GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE ROUTINE, ALTER ROUTINE, TRIGGER ON test.* TO 'checksum'@'%';
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
[██████████████████████████████████████████████████]100%  Processing:     100/100    Elapsed: 0.06s
table sbtest.t2 checksum completed
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

虽然在 Diffs 列中提示部分存储函数存在差异，但却都无法生成修复SQL，需要DBA介入判断后进行修复。

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

## repairDB自动修复工具使用说明

### 工具简介

**repairDB** 工具用于执行SQL修复文件，支持批量执行SQL文件并自动处理事务。

### 编译方法

**repairDB** 工具采用Go语言开发，您可以下载源码编译生成二进制文件。

编译环境要求使用golang 1.17及以上版本，请先行配置好Go编译环境。

请参考下面方法下载源码并进行编译：

```bash
$ git clone https://gitee.com/GreatSQL/gt-checksum.git
$ cd gt-checksum
$ go build -o repairDB repairDB.go
```

编译完成后，将编译好的二进制文件拷贝到系统PATH路径下，即可使用：

```bash
$ chmod +x repairDB
$ mv repairDB /usr/local/bin
```

### 使用方法

**repairDB** 支持两种使用方式：

#### 1. 直接指定目录执行

执行 `./repairDB fixsql` 表示从 `fixsql` 目录中读取SQL文件执行修复，而不读取 `gc.conf` 中的 fixFileDir 参数。

示例：

```bash
$ ./repairDB ./myfixsql
```

#### 2. 使用配置文件执行

执行 `./repairDB` 则先读取 `gc.conf` 中的配置参数 `fixFileDir`，如果该参数有相应的配置值，则从该参数值中读取SQL文件修复；如果没有相应的参数值，则报错退出。

示例：

```bash
$ ./repairDB
```

### 配置文件参数说明

**repairDB** 工具使用的配置文件与 **gt-checksum** 工具相同，主要关注以下几个参数：

| 参数名 | 类型 | 默认值 | 说明 |
|-------|------|-------|------|
| dstDSN | string | 无 | 目标数据库连接字符串，格式为 `mysql|user:password@tcp(host:port)/db?params` |
| parallelThds | int | 4 | 并行执行SQL文件的线程数 |
| fixFileDir | string | ./fixsql | 存放修复SQL文件的目录 |

### 执行流程

1. 读取配置文件或命令行参数（命令行参数只能指定fixsql所在目录，不支持指定其他参数）；
2. 连接目标数据库（dstDSN对应的数据库实例）；
3. 扫描指定目录下的所有 `.sql` 文件；
4. 优先执行包含 `-DELETE.sql` 的文件；
5. 然后执行其他SQL文件；
6. 每个SQL文件在一个独立的事务中执行；；
7. 执行完成后输出执行结果。

### 注意事项

1. **数据库权限**：执行 **repairDB** 工具的数据库账户需要具备执行SQL文件中包含的SQL语句的权限。

2. **SQL文件格式**：SQL文件可以包含多行SQL命令，工具会自动按分号分割并逐个执行。

3. **事务管理**：每个SQL文件在一个独立的事务中执行，确保所有语句要么全部成功，要么全部回滚。

4. **执行顺序**：工具会优先执行删除操作的SQL文件（x-DELETE.sql文件），然后执行其他操作的SQL文件，确保数据一致性。

5. **错误处理**：如果执行过程中遇到错误，工具会停止执行并输出错误信息。

6. **目录存在性**：工具会检查指定的 `fixFileDir` 目录是否存在，如果不存在则报错退出。

### 示例输出
程序执行过程中的输出会记录到repairDB.log文件中，示例如下：
```bash
$ ./repairDB ./myfixsql && cat ./repairDB.log
repairDB executed successfully

2026/01/29 10:00:00 Configuration information:
2026/01/29 10:00:00   DstDSN: mysql|checksum:Checksum@3306@tcp(127.0.0.1:3306)/sbtest?charset=utf8mb4
2026/01/29 10:00:00   ParallelThds: 4
2026/01/29 10:00:00   FixFileDir: ./myfixsql
2026/01/29 10:00:00   LogFile: repairDB.log
2026/01/29 10:00:00 Found 2 DELETE files, 3 other SQL files
2026/01/29 10:00:00 Starting to execute DELETE files
2026/01/29 10:00:00 Starting to execute SQL file: ./myfixsql/t1-DELETE.sql
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/t1-DELETE.sql, time taken: 10ms
2026/01/29 10:00:00 Starting to execute SQL file: ./myfixsql/t2-DELETE.sql
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/t2-DELETE.sql, time taken: 8ms
2026/01/29 10:00:00 DELETE files execution completed
2026/01/29 10:00:00 Starting parallel execution of other SQL files, concurrency: 4
2026/01/29 10:00:00 Starting to execute SQL file: ./myfixsql/t1-INSERT.sql
2026/01/29 10:00:00 Starting to execute SQL file: ./myfixsql/t2-INSERT.sql
2026/01/29 10:00:00 Starting to execute SQL file: ./myfixsql/t3-INSERT.sql
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/t1-INSERT.sql, time taken: 12ms
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/t2-INSERT.sql, time taken: 10ms
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/t3-INSERT.sql, time taken: 9ms
2026/01/29 10:00:00 Other SQL files execution completed
2026/01/29 10:00:00 All SQL files execution completed
```

## 已知缺陷/问题

截止最新的v1.2.4版本，已知存在以下几个约束/问题。

- 为了安全起见，当设置checkObject=data之外的其他值时，即便同时设置datafix=table，也不会直接在线完成修复，需要改成datafix=file，生成fix SQL后再由DBA手动完成。

- 当设置checkObject=trigger或routine时，如果连接数据库的账号没有相应的权限而无法读取到元数据，会导致检查结果不准确。这种情况下，先授予相应权限就可以。

- 因元数据间存在较多不一致，目前主要支持MySQL 8.0/GreatSQL 8.0版本，暂不支持跨5.7和8.0之间的校验。

- 不支持对非InnoDB引擎表的数据校验。

- 不支持数据库名、表名等数据对象名为**gtchecksum**。

- 当添加的字段是主键/外键约束字段或包含索引时，会多一个额外的`ADD PRIMARY KEY/ADD CONSTRAINT/ADD KEY`操作，需要手动删掉，或者执行时加上"-f"强制忽略错误即可。

- 当表的partition定义生成报告（Diffs=no）但不生成fixSQL（生成提示信息，没有具体SQL，需要DBA手动调整修复）。

## 问题反馈

可以 [提交issue](https://gitee.com/GreatSQL/gt-checksum/issues) 查看或提交 gt-checksum 相关bug。
