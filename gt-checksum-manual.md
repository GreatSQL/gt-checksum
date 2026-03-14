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

    如果是MySQL 8.0及以上版本，需授予 `REPLICATION CLIENT`, `SESSION_VARIABLES_ADMIN`, `SHOW_ROUTINE`, `TRIGGER` 权限。如果MySQL 5.7及以下版本，则无需授予 `SESSION_VARIABLES_ADMIN` 权限。

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

### checkObject 权限矩阵（MySQL & Oracle）

`checkObject` 参数可选值为 `data`、`struct`、`routine`、`trigger`，默认值为 `data`。

下表基于当前版本代码实现路径梳理了各模式最小建议权限、权限来源及版本差异：

| checkObject | MySQL 所需权限（名称 / 来源 / 说明） | Oracle 所需权限（名称 / 来源 / 说明） | 版本差异与说明 |
|---|---|---|---|
| `data` | 1) `REPLICATION CLIENT`（系统权限，程序启动时检查）<br>2) `SESSION_VARIABLES_ADMIN`（系统权限，程序启动时检查）<br>3) `SELECT`（对象权限，表/库/全局任一层级可覆盖）<br>4) 若 `datafix=table`：`INSERT`、`DELETE`、`ALTER`（对象权限） | 1) `SELECT ANY DICTIONARY`（系统权限，程序启动时检查）<br>2) `SELECT`（对象权限）<br>3) 若 `datafix=table`：`INSERT`、`DELETE`（对象权限）或 `INSERT ANY TABLE`、`DELETE ANY TABLE`（系统权限） | MySQL 5.7 无 `SESSION_VARIABLES_ADMIN`；MySQL 8.0 及以上建议授予。Oracle 12c+ 存在 `READ` 对象权限，但当前实现按 `SELECT` 语义检查。 |
| `struct` | 程序仍执行全局权限检查；结构比对会读取 `INFORMATION_SCHEMA.COLUMNS`、`INFORMATION_SCHEMA.STATISTICS`、`INFORMATION_SCHEMA.PARTITIONS`、`INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`等。建议至少具备目标对象与上述元数据表 `SELECT` 权限。 | 程序仍执行全局权限检查；结构比对会读取 `DBA_TAB_COLUMNS`、`DBA_COL_COMMENTS`、`USER_CONSTRAINTS`、`ALL_TABLES`，并调用 `DBMS_METADATA.GET_DDL('TABLE',...)`。建议具备 `SELECT ANY DICTIONARY` 及元数据访问能力。 | 当前实现中，`checkObject=struct` 已合并执行表结构、索引、分区、外键检查。 |
| `routine` | 读取 `INFORMATION_SCHEMA.PARAMETERS`、`INFORMATION_SCHEMA.ROUTINES`。为确保可读取完整定义，建议授予 `SHOW_ROUTINE`（系统权限）或等效的全局读取能力。 | 读取 `ALL_PROCEDURES`，并调用 `DBMS_METADATA.GET_DDL('PROCEDURE'/'FUNCTION',...)`。建议具备 `SELECT ANY DICTIONARY` 与 `DBMS_METADATA` 访问能力。 | MySQL 8.0.20+ 引入 `SHOW_ROUTINE` 权限语义更清晰；低版本通常通过更高权限覆盖。 |
| `trigger` | 读取 `INFORMATION_SCHEMA.TRIGGERS`，并执行 `SHOW CREATE TRIGGER`。建议授予 `TRIGGER`（对象权限）。 | 读取 `ALL_TRIGGERS`，并调用 `DBMS_METADATA.GET_DDL('TRIGGER',...)`。建议具备 `SELECT ANY DICTIONARY` 与元数据访问能力。 | Oracle 11g/12c/19c/23c 在 `ALL_TRIGGERS` 视图语义上基本一致（返回当前用户可访问对象）。 |

补充说明：

1. 不论 `checkObject` 取值为何，程序启动阶段都会先做全局权限检查。
2. 表级权限检查（`TableAccessPriCheck`）当前仅在 `checkObject=data` 分支中强制执行。
3. `checkObject=trigger` 或 `routine` 时，若账号无法读取对应元数据，可能出现“未报错但结果不完整”的情况，建议按上表补齐权限后再执行。
4. 当源端为 `MariaDB`、目标端为 `MySQL 8.0/8.4` 且 `checkObject=data` 时，当前版本会跳过源端 `MariaDB` 的全局权限预检查，不再要求 `SESSION_VARIABLES_ADMIN` 或 `REPLICATION CLIENT` 形式的 `MySQL` 权限名称；但仍需确保源端表具备 `SELECT` 权限，目标端 `MySQL` 侧继续按数据校验/修复路径检查相应权限。

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

如果参数 `checkObject` 设置为 **routine** 或 **trigger**，则会输出对应对象的差异结果；在 MySQL -> MySQL 场景下，当前版本也支持生成对应的 fixSQL，但这类 fixSQL 通常包含 `DROP + CREATE PROCEDURE/FUNCTION/TRIGGER` 语句，因此执行前需要额外关注目标库中的 `DEFINER` 账号与权限是否满足要求，例如：

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

虽然在 Diffs 列中可以看到部分存储程序存在差异，但对这类对象的修复不能只关注定义文本本身，还必须同时关注执行环境。尤其是当生成的 fixSQL 中包含 `DROP + CREATE PROCEDURE/FUNCTION/TRIGGER` 时，目标库必须预先存在源端定义中的 `DEFINER` 账号，并且该账号具备相应对象权限，否则执行 fixSQL 时会失败。这是运行环境约束，不是程序实现错误。

建议在执行这类 fixSQL 前，先做以下前置校验：

1. 在生成的 fixSQL 中检查是否存在 `DEFINER=` 子句；
2. 在目标库中确认对应账号已经存在；
3. 确认该账号具备创建/修改对应 `PROCEDURE`、`FUNCTION`、`TRIGGER` 所需权限；
4. 若源端与目标端账号体系不同，先由 DBA 完成账号和授权准备，再执行 `repairDB`。

## MySQL / MariaDB 跨版本兼容说明

### 支持范围

当前版本对 `MySQL` 的支持上限为 `8.4 LTS`，并按以下规则执行兼容性校验：

| 场景 | `checkObject=data` | `checkObject=struct` | 说明 |
|---|---|---|---|
| 源端与目标端同版本主线（`5.6`、`5.7`、`8.0`、`8.4`） | 支持 | 支持 | 同时支持数据校验/修复和表结构校验/修复。 |
| 源端版本主线小于目标端版本主线，且两端均在 `5.6`、`5.7`、`8.0`、`8.4` 范围内 | 支持 | 支持 | 例如 `5.6 -> 5.7`、`5.6 -> 8.0`、`5.7 -> 8.0`、`8.0 -> 8.4`。 |
| 源端为 `MariaDB 10.x+`，目标端为 `MySQL 8.0/8.4` | 支持 | 支持 | `struct` 当前仅覆盖安全子集，见下方“`checkObject=struct` 的支持边界”。 |
| 源端为 `MariaDB 10.x+`，目标端为 `MySQL 8.0` 以下版本 | 不支持 | 不支持 | 程序会在启动阶段直接退出，并提示当前组合不受支持。 |
| 源端为 `MySQL`，目标端为 `MariaDB` | 不支持 | 不支持 | 程序会在启动阶段直接退出，并提示当前组合不受支持。 |
| 源端与目标端均为 `MariaDB` | 不支持 | 不支持 | 当前版本未纳入正式支持范围。 |
| 源端版本主线大于目标端版本主线 | 不支持 | 不支持 | 程序会在启动阶段直接退出，并明确提示 downgrade 场景不受支持。 |
| 任一端版本主线不在 `5.6`、`5.7`、`8.0`、`8.4` 范围内 | 不支持 | 不支持 | 程序会在启动阶段直接退出，并提示支持的版本范围。 |

### `checkObject=data` 的前置条件

1. 源端与目标端 `srcDSN`、`dstDSN` 中的 `charset` 参数必须一致；如果两端字符集不一致，程序会在启动阶段直接退出，避免出现数据校验结果失真或修复后乱码的问题。
2. 当源端为 `MariaDB` 时，仅支持 `MariaDB 10.x+ -> MySQL 8.0/8.4` 的数据校验/修复路径；其他 `MariaDB` 组合仍会在启动阶段直接拒绝执行。
3. 当数据校验前发现表结构不一致时，程序不会继续做该表的数据比对，而是保留结果并将 `Diffs` 标记为 `DDL-yes`。如果需要进一步修复表结构，请改用 `checkObject=struct`。

### `checkObject=struct` 的支持边界

当前 `checkObject=struct` 的能力边界如下：

1. `MySQL -> MySQL`
   - 已覆盖普通列、默认值、`charset/collation`、`PRIMARY KEY`、`UNIQUE`、普通索引、外键、`CHECK` 风险输出；
   - 已内置 `utf8 -> utf8mb3`、整数显示宽度、`ZEROFILL`、`ROW_FORMAT` 默认漂移、默认 `utf8mb4` 排序规则漂移等归一化规则；
   - `CHECK`、高风险外键不会自动执行高风险 DDL，而是保留为 `warn-only` 或 advisory 信息。
2. `MariaDB -> MySQL 8.0/8.4`
   - 已覆盖安全子集：`JSON`、generated columns、`INET6`、`UUID`、`COMPRESSED`、`IGNORED INDEX`；
   - `MariaDB JSON` 可通过 `mariaDBJSONTargetType` 配置为 `JSON`、`LONGTEXT` 或 `TEXT`；
   - `COMPRESSED`、`MariaDB JSON -> LONGTEXT/TEXT` 的语义降级会保留为 `warn-only`。
3. 以下对象当前只做识别、告警和 advisory 输出，不自动修复：
   - `SYSTEM VERSIONING`
   - `WITHOUT OVERLAPS`
   - `SEQUENCE`

### 结构迁移专项参数

| 参数名 | 可选值 | 默认值 | 说明 |
|---|---|---|---|
| `mariaDBJSONTargetType` | `JSON` / `LONGTEXT` / `TEXT` | `JSON` | 控制 `MariaDB JSON` alias 在 `MariaDB -> MySQL 8.0/8.4` 结构迁移时的目标列类型。`JSON` 语义最接近；`LONGTEXT` 适合作为兼容性保底；`TEXT` 当前已实现但未纳入发布级实库基线。 |
| `fixFilePerTable` | `ON` / `OFF` | `OFF` | 结构迁移场景建议设为 `ON`，便于逐表审查 fix SQL 与 advisory SQL。 |
| `datafix` | `file` / `table` | `file` | `checkObject=struct` 场景建议固定为 `file`，先生成 fix SQL 供 DBA 审查，再使用 `repairDB` 回放。 |

### 推荐配置示例

以下示例表示执行 `MySQL 5.7 -> MySQL 8.0` 的数据校验，并要求两端统一使用 `utf8mb4`：

```ini
srcDSN = mysql|checksum:Checksum@3306@tcp(src-mysql57-host:3405)/information_schema?charset=utf8mb4
dstDSN = mysql|checksum:Checksum@3306@tcp(dst-mysql80-host:3406)/information_schema?charset=utf8mb4
tables = gt_checksum.*
checkObject = data
datafix = file
```

以下示例表示执行 `MySQL 5.7 -> MySQL 8.0` 的表结构校验与修复 SQL 生成：

```ini
srcDSN = mysql|checksum:Checksum@3306@tcp(src-mysql57-host:3405)/information_schema?charset=utf8mb4
dstDSN = mysql|checksum:Checksum@3306@tcp(dst-mysql80-host:3406)/information_schema?charset=utf8mb4
tables = gt_phase1_mysql57.*
checkObject = struct
datafix = file
fixFilePerTable = ON
fixFileDir = ./fixsql-struct-mysql57-to80
```

以下示例表示执行 `MariaDB 10.5 -> MySQL 8.0` 的安全子集表结构校验与 fix SQL 生成，并将 `JSON` alias 降级为 `LONGTEXT`：

```ini
srcDSN = mysql|checksum:Checksum@3306@tcp(src-mariadb105-host:3407)/information_schema?charset=utf8mb4
dstDSN = mysql|checksum:Checksum@3306@tcp(dst-mysql80-host:3406)/information_schema?charset=utf8mb4
tables = gt_phase1_mariadb105.*
checkObject = struct
datafix = file
fixFilePerTable = ON
fixFileDir = ./fixsql-struct-mariadb105-to80
mariaDBJSONTargetType = LONGTEXT
```

### 结构迁移标准操作步骤

建议按以下步骤执行 `checkObject=struct`：

1. 首轮 compare：执行 `gt-checksum -c ...` 生成 fix SQL 与 advisory SQL；
2. 人工审查：检查 `fixFileDir` 中的 SQL 文件，确认映射表名、`warn-only` 对象和 `MariaDB JSON` 目标类型是否符合预期；
3. 回放修复：使用 `repairDB` 回放 fix SQL；
4. 二次 compare：再次执行相同配置，确认结果已收敛到 `no` 或可解释的 `warn-only`。

对于当前版本，`warn-only` 通常表示以下几类可解释、可审计的残余风险：

1. `CHECK` 风险
2. `COMPRESSED`
3. `MariaDB JSON -> LONGTEXT/TEXT` 的语义降级
4. `SYSTEM VERSIONING / WITHOUT OVERLAPS / SEQUENCE` 的 advisory-only 边界

### DDL 差异结果展示

当源端与目标端表结构不一致时，结果总览中会保留该表，并按如下方式展示：

```text
Checksum Results Overview
Schema       Table    IndexColumn  CheckObject  Rows  Diffs    Datafix
gt_checksum  tb_emp6               data               DDL-yes  file
```

说明如下：

1. `Diffs=DDL-yes` 表示当前表存在 DDL 差异，当前轮次不会继续做数据比对。
2. `Rows` 列固定显示为空值，这是预期行为，用于避免列差异信息过长时破坏终端表格布局。
3. 若需查看具体差异字段，请检查运行日志；日志中会记录 `Extra=[...]`、`Missing=[...]` 等详细信息。

当结构 compare 已收敛，但仍保留明确可解释的残余风险时，结果会显示为 `warn-only`。例如：

```text
Checksum Results Overview
Schema              Table                         CheckObject  Diffs      Datafix
gt_phase1_mariadb105 t_mariadb_feature_pack      struct       warn-only  file
```

这表示：

1. 当前对象的 fix SQL 已经完成主要结构改写；
2. 剩余差异属于已知且可审计的风险边界；
3. 不再等同于“未收敛的普通结构差异”。

### repair SQL 前置语句兼容性

无论是生成 fix SQL 文件，还是执行在线修复，程序都会根据 `dstDSN` 中的 `charset` 自动生成统一的前置 `session` 语句，包括：

- `SET NAMES ...`
- `SET FOREIGN_KEY_CHECKS=0`
- `SET UNIQUE_CHECKS=0`
- `SET INNODB_LOCK_WAIT_TIMEOUT=1073741824`

对于仅在 `MySQL 8.0` 中支持的 `sql_require_primary_key` 与 `sql_generate_invisible_primary_key`，程序会使用 **MySQL versioned comments** 包裹；因此同一套 fix SQL 可以兼容 `MySQL 5.6/5.7/8.0/8.4`，低版本实例会自动忽略不支持的 `session` 变量设置。

### MariaDB 源端权限检查说明

当场景为 `MariaDB 10.x+ -> MySQL 8.0/8.4` 且 `checkObject=data` 或 `checkObject=struct` 时，程序的权限检查行为如下：

1. 源端 `MariaDB`：跳过全局权限预检查，不再要求 `SESSION_VARIABLES_ADMIN`、`REPLICATION CLIENT` 这类 `MySQL` 命名的全局权限；
2. 目标端 `MySQL 8.0/8.4`：继续按现有逻辑检查全局权限；
3. 源端与目标端表级权限：`checkObject=data` 仍按既有逻辑检查 `SELECT` 以及 `datafix=table` 时所需的对象权限；`checkObject=struct` 则需确保可读取相关元数据并具备目标端执行 fix SQL 所需的对象级 DDL 权限；
4. 如果终端提示 `Missing required global privileges`，请优先打开 debug 日志，根据日志中源端/目标端各自的权限检查结果确认具体缺失项，而不要仅凭终端输出中的概括性提示判断。

## 配置参数详解

**gt-checksum** 支持命令行参数与配置文件方式运行，但命令行仅支持 `-c/-f`, `-h`, `-v` 等基础参数，其余参数通过配置文件指定。

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
$ CGO_ENABLED=0 go build -o repairDB repairDB.go
```

编译完成后，将编译好的二进制文件拷贝到系统PATH路径下，即可使用：
```bash
$ chmod +x gt-checksum
$ mv gt-checksum /usr/local/bin
```

## repairDB自动修复工具使用说明

### 工具简介

**repairDB** 工具用于执行SQL修复文件，支持批量执行SQL文件并自动处理事务。针对并行修复过程中可能出现的死锁冲突（MySQL Error 1213），内置自动重试机制：按 `BEGIN ... COMMIT` 事务块进行重试，最多 3 次。

### 编译方法

**repairDB** 工具采用Go语言开发，您可以下载源码编译生成二进制文件。

编译环境要求使用golang 1.17及以上版本，请先行配置好Go编译环境。

请参考下面方法下载源码并进行编译：

```bash
$ git clone https://gitee.com/GreatSQL/gt-checksum.git
$ cd gt-checksum
$ CGO_ENABLED=0 go build -o repairDB repairDB.go
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
6. 将SQL文件拆分为执行单元：普通语句单独执行，`BEGIN ... COMMIT/ROLLBACK` 作为一个事务块执行；
7. 若检测到死锁错误（Error 1213），仅对当前失败事务块自动重试，最多 3 次（指数退避），不重试整个SQL文件；
8. 执行完成后输出执行结果。

### 注意事项

1. **数据库权限**：执行 **repairDB** 的数据库账户需要具备执行SQL文件中包含的SQL语句的权限。

2. **SQL文件格式**：SQL文件可以包含多行SQL命令，会自动按分号分割并逐个执行。

   对于包含 `PROCEDURE`、`FUNCTION`、`TRIGGER` 定义的 fixSQL，`repairDB` 也支持解析 `DELIMITER` 语法并执行。但如果脚本中包含 `DROP + CREATE PROCEDURE/FUNCTION/TRIGGER`，则目标库仍需预先满足对应 `DEFINER` 账号与权限要求。

3. **事务管理**：按SQL执行单元处理事务。`BEGIN ... COMMIT` 内的语句作为一个事务块执行，失败时回滚该事务块；普通语句按语句级执行。

4. **执行顺序**：优先执行删除操作的SQL文件（x-DELETE.sql文件），然后执行其他操作的SQL文件，确保数据一致性。为了降低同一个表上的锁等待，在执行删除操作类和其他类的SQL文件时，采用随机并行执行的方式。

5. **错误处理**：遇到死锁错误（Error 1213）时会自动重试当前失败事务块，最多 3 次；若仍失败或遇到非死锁错误，则停止并输出错误信息。

6. **目录存在性**：检查指定的 `fixFileDir` 目录是否存在，如果不存在则报错退出。

7. **并行执行**：以 `parallelThds` 线程数并行执行SQL文件。为了提高数据修复效率，可以考虑临时加大 `parallelThds` 参数值，但同时需要注意对目标数据库的负载的影响和死锁概率。

8. **DEFINER 前置检查（可选但强烈建议）**：当 fixSQL 中包含 `CREATE DEFINER=... PROCEDURE/FUNCTION/TRIGGER` 时，建议在执行前先扫描 fixSQL 并核对目标库账号体系。例如先确认脚本里引用了哪些 `DEFINER`，再确认目标库是否已存在对应账号及授权。若缺失账号或权限，应先由 DBA 补齐，再执行 `repairDB`。

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

这就表示完成修复，可以再次执行数据校验，确认数据一致性。

**注意**：由于是并行执行数据修复工作，修复过程中可能产生死锁冲突。`repairDB` 在检测到 MySQL deadlock（Error 1213）时，会自动对当前失败事务块（`BEGIN ... COMMIT`）进行重试，最多 3 次，不重试整个SQL文件。建议在修复结束后检查 `repairDB.log`：若死锁已在重试内恢复，可直接再次校验；若仍有死锁或其他错误，则手动处理对应SQL文件。

## oracle_random_data_load 工具使用说明

### 工具定位

`oracle_random_data_load` 是面向 Oracle 的随机数据写入工具，适用于以下场景：

1. 构造测试数据（功能测试、回归测试）；
2. 构造压测数据（并发写入、批量写入验证）；
3. 在迁移校验前快速填充目标表，验证链路稳定性。

该工具会读取目标表元数据，按字段类型自动生成随机值，并通过批量 `INSERT ALL` 提升大数据量写入效率。

### 核心能力

1. 自动识别列信息：通过 Oracle 元数据获取列名、类型、精度、是否可空、主键等信息；
2. 主键优先唯一化：对主键列优先采用“唯一值计划”，降低 `ORA-00001` 冲突概率；
3. 并发批量写入：多 worker 并发 + `INSERT ALL ... SELECT 1 FROM DUAL`；
4. 失败自动降级：批次失败后重试，仍失败则自动退化为逐行写入；
5. 进度与统计：周期输出进度，结束输出 summary（目标行数、成功/失败、重试次数、吞吐等）。

### 使用前准备

1. 准备好 Oracle Client（Instant Client）并确保 `godror` 可正常连接；
2. 执行账号需至少具备目标表的写入权限（`INSERT`）以及读取表元数据的权限；
3. 建议优先使用表 owner 账号执行，减少元数据可见性导致的问题；
4. 若目标表已存在大量历史数据，建议先评估主键冲突风险（可先清空或使用独立测试表）。

### 编译与运行

```bash
go build -o oracle_random_data_load oracle_random_data_load.go
```

```bash
./oracle_random_data_load \
  -dsn 'user="checksum" password="checksum" connectString="127.0.0.1:1521/gtchecksum" timezone="Asia/Shanghai" noTimezoneCheck="true"' \
  -table gtchecksum.t1 \
  -rows 10000
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `-dsn` | string | 无 | Oracle 连接串（godror 格式，必填） |
| `-schema` | string | 空 | schema 名，和 `-table` 组合使用 |
| `-table` | string | 无 | 表名（支持 `TABLE` 或 `SCHEMA.TABLE`，必填） |
| `-table-full` | string | 空 | `SCHEMA.TABLE` 别名参数，设置后覆盖 `-table` |
| `-rows` | int64 | `10000` | 目标写入总行数 |
| `-workers` | int | `4` | 并发 worker 数 |
| `-batch-size` | int | `200` | 单批写入行数（上限 `100000`，并受 Oracle 绑定变量上限动态约束） |
| `-max-retries` | int | `2` | 批次失败后的重试次数 |
| `-null-rate` | float64 | `0.10` | 可空列写入 `NULL` 的比例，范围 `[0,1]` |
| `-progress-interval` | int | `2` | 进度日志输出间隔（秒） |
| `-exec-timeout` | int | `30` | 单批执行超时时间（秒） |
| `-time-range-days` | int | `3650` | 日期时间随机范围（向前回溯天数） |
| `-seed` | int64 | 当前时间戳 | 随机种子，固定后可复现同分布数据 |
| `-log-file` | string | 空 | 日志输出文件（空则仅 stdout） |
| `-continue-on-error` | bool | `true` | 出错后是否继续运行 |
| `-exclude-columns` | string | 空 | 排除列列表（逗号分隔，如 `ID,CREATE_TIME`） |
| `-print-sql` | bool | `false` | 打印生成的 `INSERT ALL` SQL 模板 |
| `-db-max-open-conns` | int | `0` | 连接池最大连接数（`0` 表示自动） |
| `-db-max-idle-conns` | int | `0` | 连接池最大空闲连接数（`0` 表示自动） |
| `-db-conn-max-lifetime-minutes` | int | `30` | 连接最大生命周期（分钟） |

> 说明：工具会将 schema/table 统一按 Oracle 规则处理（默认转大写）。如果使用 `-table SCHEMA.TABLE`，可不再单独设置 `-schema`。

### 数据生成规则（按类型）

1. `NUMBER`：按精度和小数位生成整数或小数；
2. `FLOAT/BINARY_FLOAT/BINARY_DOUBLE`：生成随机浮点值；
3. `CHAR/VARCHAR2/NCHAR/NVARCHAR2`：按字符语义或字节语义生成并自动裁剪，避免超长；
4. `DATE/TIMESTAMP`：在 `time-range-days` 指定区间内生成随机时间；
5. `CLOB`：生成随机文本段；
6. `BLOB/RAW/LONG RAW`：生成随机字节并按列长度裁剪。

对主键列会优先构建唯一值生成计划（如基于当前 `MAX(pk)` 加步长），显著减少主键冲突。

### 执行流程

1. 解析参数并校验；
2. 连接 Oracle 并加载列元数据与主键信息；
3. 过滤不可写/不支持列，构建随机值生成器；
4. 按列数和 Oracle 绑定变量上限（`65535`）自动调整可用批大小；
5. 多 worker 并发生成数据并执行批量写入；
6. 批次失败时按重试策略执行，必要时降级逐行写入；
7. 输出进度日志和最终 summary。

### 快速案例

#### 案例1：最小可用

```bash
./oracle_random_data_load \
  -dsn 'user="checksum" password="checksum" connectString="127.0.0.1:1521/gtchecksum" timezone="Asia/Shanghai" noTimezoneCheck="true"' \
  -table gtchecksum.t1 \
  -rows 1000
```

#### 案例2：固定种子 + 排除列

```bash
./oracle_random_data_load \
  -dsn 'user="checksum" password="checksum" connectString="127.0.0.1:1521/gtchecksum" timezone="Asia/Shanghai" noTimezoneCheck="true"' \
  -schema gtchecksum \
  -table t1 \
  -rows 50000 \
  -workers 4 \
  -batch-size 300 \
  -seed 20260306 \
  -exclude-columns CREATED_AT,UPDATED_AT
```

#### 案例3：压测模式（严格失败即退出）

```bash
./oracle_random_data_load \
  -dsn 'user="checksum" password="checksum" connectString="127.0.0.1:1521/gtchecksum" timezone="Asia/Shanghai" noTimezoneCheck="true"' \
  -table gtchecksum.t1 \
  -rows 2000000 \
  -workers 8 \
  -batch-size 800 \
  -max-retries 3 \
  -continue-on-error=false \
  -db-max-open-conns 32 \
  -db-max-idle-conns 16 \
  -db-conn-max-lifetime-minutes 20 \
  -log-file ./oracle_random_data_load.log
```

### 日志与结果解读

执行过程中会看到类似进度日志：

```text
progress=62.50% generated=125000 inserted=124900 failed=100 ok_batches=312 fail_batches=2 retries=5(batch=4,row=1) rate=10234.5 rows/s
```

结束时会输出 summary：

```text
========== oracle_random_data_load summary ==========
target: gtchecksum.t1
rows target=200000 generated=200000 inserted=199998 failed=2
batches ok=399 failed=1 retries=3(batch=2,row=1)
elapsed=18.254s throughput=10956.4 rows/s
result=PARTIAL_SUCCESS continue_on_error=true
```

### 常见问题与处理建议

1. `ORA-12899`（字段超长）  
   原因：随机字符串超过列长度（常见于 `NCHAR/NVARCHAR2`）。  
   处理：检查列长度定义、确认字符语义，必要时用 `-exclude-columns` 排除特殊列，或降低字符串长度来源。

2. `ORA-00001`（唯一约束冲突）  
   原因：目标表已存在数据、主键生成范围重叠，或特殊主键类型被截断后冲突。  
   处理：清理测试数据后重跑，或调整主键列设计/排除冲突列，必要时分表加载。

3. `ORA-00257`（归档空间满）  
   原因：Oracle FRA / archive log 空间不足。  
   处理：联系 DBA 清理归档或扩容后重试。

4. `godror WARNING: discrepancy between DBTIMEZONE and SYSTIMESTAMP`  
   原因：数据库时区与会话时区不一致。  
   处理：在 DSN 中显式设置 `timezone`，必要时设置 `noTimezoneCheck=true`。

### 性能调优建议

1. 优先调 `workers` 与 `batch-size`，再调连接池参数；
2. 避免将 `batch-size` 设置过大，受 Oracle bind 变量上限影响会被自动降级；
3. 压测时建议固定 `seed`，便于结果复现与横向对比；
4. 对大量 LOB 列的表，建议分阶段加载或适度排除非关键列，降低 I/O 压力。

## 已知缺陷/问题

截止最新的v1.2.5版本，已知存在以下几个约束/问题。

- 当存在触发器时，因为触发器的作用，可能导致在修复完一个表后，触发其他表被改变，从而看起来像是修复后仍不一致的情况。这种情况下，需要先临时删除触发器进行修复，完成后在重新创建触发器。

- 为了安全起见，当设置checkObject=data之外的其他值时，即便同时设置datafix=table，也不会直接在线完成修复，需要改成datafix=file，生成fix SQL后再由DBA手动完成。

- 当设置checkObject=trigger或routine时，如果连接数据库的账号没有相应的权限而无法读取到元数据，会导致检查结果不准确。这种情况下，先授予相应权限就可以。

- 当 `checkObject=trigger` 或 `routine` 生成的 fixSQL 中包含 `DROP + CREATE PROCEDURE/FUNCTION/TRIGGER` 时，目标库必须预先存在源端定义中的 `DEFINER` 账号及权限，否则执行会失败。这是环境约束，不是程序实现错误。建议在执行 `repairDB` 前，先对 fixSQL 中的 `DEFINER` 做一次人工检查。

- 已支持 `MySQL 5.6`、`5.7`、`8.0`、`8.4` 的同版本和升级链路校验；但仍不支持 `src > dst` 的 downgrade 场景，程序会在启动阶段直接退出。

- 当 `checkObject=data` 且两端 DSN 中的 `charset` 参数不一致时，程序会在启动阶段直接拒绝执行；如需继续校验，请先统一连接字符集配置。

- `MariaDB` 当前仅支持作为源端，目标端为 `MySQL 8.0/8.4`；其中 `checkObject=struct` 仅覆盖安全子集。`MariaDB -> MySQL 8.0` 以下版本、`MySQL -> MariaDB` 以及 `MariaDB -> MariaDB` 组合仍不受支持。

- `MariaDB JSON -> TEXT` 虽已具备规则改写和单测覆盖，但当前尚未纳入发布级实库基线；如需使用，建议先在测试环境中自行完成 fix SQL 回放与二次 compare 验证。

- `SYSTEM VERSIONING`、`WITHOUT OVERLAPS`、`SEQUENCE` 当前只会输出 `warn-only` 或 advisory 信息，不会自动生成可直接执行的迁移 SQL。

- 不支持对非InnoDB引擎表的数据校验。

- 当添加的字段是主键/外键约束字段或包含索引时，会多一个额外的`ADD PRIMARY KEY/ADD CONSTRAINT/ADD KEY`操作，需要手动删掉，或者执行时加上"-f"强制忽略错误即可。

- 当表的partition定义生成报告（Diffs=no）但不生成fixSQL（生成提示信息，没有具体SQL，需要DBA手动调整修复）。

## 问题反馈

可以 [提交issue](https://gitee.com/GreatSQL/gt-checksum/issues) 查看或提交 gt-checksum 相关bug。
