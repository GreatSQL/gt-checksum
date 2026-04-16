# gt-checksum 手册

## 关于gt-checksum

**gt-checksum** 是GreatSQL社区开源的数据库校验及修复工具，支持 MySQL-family（MySQL/Percona/GreatSQL/MariaDB等）、Oracle 等主流数据库。

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
| `struct` | 程序仍执行全局权限检查；结构比对会读取 `INFORMATION_SCHEMA.COLUMNS`、`INFORMATION_SCHEMA.STATISTICS`、`INFORMATION_SCHEMA.PARTITIONS`、`INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`等。建议至少具备目标对象与上述元数据表 `SELECT` 权限。**若校验对象中包含视图（VIEW），还需额外授予 `SHOW VIEW` 权限**（MySQL 5.7+），否则 `SHOW CREATE VIEW` 会报 `Error 1142: SHOW VIEW command denied`。 | 程序仍执行全局权限检查；结构比对会读取 `DBA_TAB_COLUMNS`、`DBA_COL_COMMENTS`、`USER_CONSTRAINTS`、`ALL_TABLES`，并调用 `DBMS_METADATA.GET_DDL('TABLE',...)`。建议具备 `SELECT ANY DICTIONARY` 及元数据访问能力。 | 当前实现中，`checkObject=struct` 已合并执行表结构、索引、分区、外键检查；VIEW 专项支持仅限 MySQL→MySQL 场景。 |
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
db1     test2                           NULL            data            0,0             no      file    Schema: db1.test2:db2.test2
db1     indext                          id              data            0,0             no      file    Schema: db1.indext:db2.indext
db1     tb_emp6                         id              data            0,0             no      file    Schema: db1.tb_emp6:db2.tb_emp6
sbtest  sbtest2                         id              data            4999,4999       yes     file    -
db1     testbin                         NULL            data            1,1             no      file    Schema: db1.testbin:db2.testbin
```

输出结果中，除了 **sbtest.sbtest2** 这个表所在行中 **Mapping** 列的值为 **-** 外，其他表的 **Mapping** 列都会显示成 **Schema: 源端库.源端表:目标库.目标表** 的形式，例如 **Schema: db1.test2:db2.test2**，表示该表在源端和目标端的实际映射关系。

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

| 场景 | `data` | `struct` | `routine` | `trigger` | 说明 |
|---|---|---|---|---|---|
| 源端与目标端同版本主线（`5.6`、`5.7`、`8.0`、`8.4`） | 支持 | 支持 | 支持 | 支持 | 同时支持数据、表结构、存储程序和触发器的校验与修复。 |
| 源端版本主线小于目标端版本主线，且两端均在 `5.6`、`5.7`、`8.0`、`8.4` 范围内 | 支持 | 支持 | 支持 | 支持 | 例如 `5.6 -> 5.7`、`5.6 -> 8.0`、`5.7 -> 8.0`、`8.0 -> 8.4`。 |
| 源端为 `MariaDB 10.x+`，目标端为 `MySQL 8.0/8.4` | 支持 | 支持 | 支持 | 支持 | `struct` 仅覆盖安全子集；`routine`/`trigger` 已支持 charset 元数据三维度比对。 |
| 源端与目标端均为 `MariaDB`（同序列或升级，支持系列见下方说明） | 支持 | 支持 | 支持 | 支持 | 仅支持升级方向（src ≤ dst），不支持 downgrade；struct fix 的隐藏索引使用 `IGNORED` 关键字；COMPRESSED/PERSISTENT 等原生列属性会在目标端保留；routine/trigger 元数据比对已开放。 |
| 源端为 `Oracle`，目标端为 `MySQL 8.0/8.4` | 支持 | 支持 | 不支持 | 不支持 | `data` 模式支持 `CHAR`/`NCHAR` 尾部空格归一化、`FLOAT`/`BINARY_FLOAT` float32 精度归一化；`struct` 模式支持目标端缺表时自动生成 `CREATE TABLE`（含主键）、列类型规范化比对（`VARCHAR2`/`NUMBER`/`TIMESTAMP`/`DATE`/`FLOAT`/`CLOB`/`BLOB` 等完整映射）、列名大小写差异忽略。 |
| 源端为 `MariaDB 10.x+`，目标端为 `MySQL 8.0` 以下版本 | 不支持 | 不支持 | 不支持 | 不支持 | 程序会在启动阶段直接退出，并提示当前组合不受支持。 |
| 源端为 `MySQL`，目标端为 `MariaDB` | 不支持 | 不支持 | 不支持 | 不支持 | 程序会在启动阶段直接退出，并提示当前组合不受支持。 |
| 源端版本主线大于目标端版本主线 | 不支持 | 不支持 | 不支持 | 不支持 | 程序会在启动阶段直接退出，并明确提示 downgrade 场景不受支持。 |
| 任一端版本主线不在 `5.6`、`5.7`、`8.0`、`8.4` 范围内（MariaDB→MariaDB 另行说明） | 不支持 | 不支持 | 不支持 | 不支持 | 程序会在启动阶段直接退出，并提示支持的版本范围。 |

**MariaDB→MariaDB 支持的系列**：`10.0`、`10.1`、`10.2`、`10.3`、`10.4`、`10.5`、`10.6`、`10.11`、`11.4`、`11.5`、`12.3`。两端均须在此列表内且源端系列 ≤ 目标端系列；不在列表内的系列会在启动阶段直接退出。各系列特性能力（JSON、不可见列、函数式索引、CHECK 约束强制执行、COMPRESSED 列属性等）按实际引入版本自动门控，详见下表：

| 特性 | 引入版本 |
|------|---------|
| JSON 数据类型（longtext+JSON_VALID alias） | 10.2 |
| CHECK 约束强制执行 | 10.2 |
| 不可见列（INVISIBLE COLUMN） | 10.3 |
| COMPRESSED 列属性 | 10.3 |
| 函数式/表达式索引 | 10.4 |
| IGNORED（不可见）索引 | 10.6 |
| INET6 原生类型 | 10.5 |
| UUID 原生类型 | 10.7 |

### `tables` / `ignoreTables` 参数使用注意事项

**通配符限制**：表名段仅支持 `%` 作为部分通配符（如 `db1.t%`）；`*` 只在 `db.*` 形式（表示某库下所有表）时有效，**不支持**将 `*` 置于表名中间或末尾（如 `db.t*`、`db.prefix_*`）。若误用此类模式，程序会在参数校验阶段立即退出并打印错误提示，例如：

```text
gt-checksum: tables option 'sbtest.t*' uses unsupported wildcard '*'; use '%' instead, e.g. sbtest.t%
```

该检查同时覆盖映射规则的目标侧（如 `db1.t%:db2.t*` 中的 `db2.t*`）以及 `ignoreTables` 参数。

**表不存在时的输出行为**：当 `tables` 指定的表在源端或两端均不存在时，程序会在结果表格中输出一行对应记录，`Diffs=yes`、`Datafix=file`，`CheckObject` 列显示用户实际配置的校验模式（`data` 或 `struct`），不再硬编码为 `struct`。

### `checkObject=data` 的前置条件

1. 源端与目标端 `srcDSN`、`dstDSN` 中的 `charset` 参数必须一致；如果两端字符集不一致，程序会在启动阶段直接退出，避免出现数据校验结果失真或修复后乱码的问题。
2. 当源端为 `MariaDB` 时，仅支持 `MariaDB 10.x+ -> MySQL 8.0/8.4` 的数据校验/修复路径；其他 `MariaDB` 组合仍会在启动阶段直接拒绝执行。
3. 当数据校验前发现表结构不一致时，程序不会继续做该表的数据比对，而是保留结果并将 `Diffs` 标记为 `DDL-yes`。如果需要进一步修复表结构，请改用 `checkObject=struct`。
4. **连接池容量**：`data` 模式下，程序内部同时运行 `queryTableDataSeparate`（checksum 比较）与 `AbnormalDataDispos`（差异处理）两条并发 pipeline，各自最多占用 `parallelThds` 个连接，单侧峰值约为 `parallelThds*2 + 2`。程序自动按 `parallelThds*2 + 4`（最低 8）设置单侧连接池下限，请确保数据库 `max_connections` 足以承载 `(parallelThds*2 + 4) * 2`（源端+目标端）个连接。`struct`/`routine`/`trigger` 模式单侧固定为 3 个连接，与 `parallelThds` 无关。

### `columns` 只校验部分字段与修复

`columns` 用于在 `checkObject=data` 模式下只比较一张逻辑表中的部分业务列，适合“目标端列名已改名，但只想核对若干关键列”的场景。

#### 功能行为

1. 程序内部会自动把主键或唯一键列追加到查询条件中，用来做行级配对；`columns` 中只需要写真正想比对的业务列。
2. **two-sided** 行（源端和目标端都存在同一主键，但选中列值不同）会生成 `UPDATE` 修复语句，只更新选中的列。
3. **source-only** 行（源端有、目标端无）不会自动生成 `INSERT`，而是写入 `columns-advisory.<schema>.<table>.sql` 提示人工处理。
4. **target-only** 行（目标端有、源端无）默认只记录差异；当 `extraRowsSyncToSource=ON` 时才会生成 `DELETE` 修复语句。
5. 终端结果和 CSV 结果都会追加 `Columns` 字段，显示本次实际参与比对的列计划。

#### 使用限制

1. 只支持 `checkObject=data`，其他模式下设置 `columns` 会在启动阶段直接报错。
2. 只支持 `MySQL-family -> MySQL-family`，当前不支持任一端为 `Oracle` 的 columns 模式。
3. 只支持“有主键或唯一键”的表；无索引表无法可靠做行级配对，会被直接跳过并标记为 `DDL-yes`。
4. `columns` 一次只对应一张逻辑表。简单语法要求 `tables` 中恰好只有一对明确的表；完整语法允许 `tables` 里存在多条规则，但 `columns` 中所有列映射必须都属于同一对源表和目标表。
5. `extraRowsSyncToSource=ON` 只能和 `columns` 一起使用，且要求 `datafix` 不是 `no`。

#### 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `columns` | 空 | 指定要比较的列；支持简单列名列表和完整列映射两种写法 |
| `extraRowsSyncToSource` | `OFF` | 仅在 `columns` 模式下生效；控制 target-only 行是否自动生成 `DELETE` |

#### `columns` 写法

1. 简单列名列表：只适用于 `tables` 中恰好指定了一张表，且源端和目标端列名相同。

```ini
tables = "testdb.orders:testdb.orders_archive"
columns = "amount,status,updated_at"
```

2. 完整列映射：适用于列名重命名、跨库映射或目标表名不同的场景。

```ini
tables = "testdb.orders:archive.orders_archive"
columns = "testdb.orders.amount:archive.orders_archive.total_amount,testdb.orders.status:archive.orders_archive.order_status"
```

#### DDL 预检规则

1. 选中列必须在两端真实存在，否则直接报错；单表任务会退出，多表任务会跳过当前表。
2. 选中列会做严格的类型兼容性检查；如果基础类型不兼容，不继续做数据校验。
3. 非选中列即使存在 DDL 差异，也只记录 `Warn` 日志，不阻断本次子集数据校验。
4. 当选中列是“源端旧列名 -> 目标端新列名”时，结构预检会按列映射豁免这类 rename，不会因为列名不同直接把整张表打成 `DDL-yes`。

#### 输出与修复文件

columns 模式下，结果中的 `Columns` 列会按如下规则展示：

1. 同名列模式：`srcSchema.srcTable.col1,srcSchema.srcTable.col2`
2. 列映射模式：`srcSchema.srcTable.srcCol:dstSchema.dstTable.dstCol`

如果存在 source-only 或未删除的 target-only 行，还会在 `fixFileDir` 目录下生成 advisory 文件，例如：

```text
fixsql/columns-advisory.testdb.orders.sql
```

文件内容全部是注释，不会被误执行。典型片段如下：

```sql
-- Source-only rows (exist in source, absent in target) : 2
--     SELECT src.* FROM `testdb`.`orders` src
--     LEFT JOIN `archive`.`orders_archive` dst USING (`id`)
--     WHERE dst.`id` IS NULL;
--
-- Target-only rows (exist in target, absent in source) : 1
--     SELECT dst.* FROM `archive`.`orders_archive` dst
--     LEFT JOIN `testdb`.`orders` src USING (`id`)
--     WHERE src.`id` IS NULL;
```

#### 使用示例

```ini
srcDSN = mysql|checksum:Checksum@3306@tcp(src-host:3306)/information_schema?charset=utf8mb4
dstDSN = mysql|checksum:Checksum@3306@tcp(dst-host:3307)/information_schema?charset=utf8mb4
tables = "testdb.orders:archive.orders_archive"
checkObject = data
datafix = file
columns = "testdb.orders.amount:archive.orders_archive.total_amount,testdb.orders.status:archive.orders_archive.order_status"
extraRowsSyncToSource = OFF
```

终端输出示例：

```text
Checksum Results Overview
Schema  Table   IndexColumn  CheckObject  Rows         Diffs  Datafix  Mapping                                      Columns
testdb  orders  id           data         1000,1000    yes    file     Schema: testdb.orders:archive.orders_archive testdb.orders.amount:archive.orders_archive.total_amount,testdb.orders.status:archive.orders_archive.order_status
```

对应的 CSV 行示例：

```text
RunID,CheckTime,CheckObject,Schema,Table,ObjectName,ObjectType,IndexColumn,Rows,Diffs,Datafix,Mapping,Definer,Columns
20260403153000,2026-04-03 15:30:01,data,testdb,orders,orders,table,id,"1000,1000",yes,file,Schema: testdb.orders:archive.orders_archive,,"testdb.orders.amount:archive.orders_archive.total_amount,testdb.orders.status:archive.orders_archive.order_status"
```

#### columns 功能回归测试

`scripts/regression-test-columns.sh` 是针对 `columns` 端到端集成回归测试脚本，配套数据 fixture 文件位于 `testcase/MySQL-columns-source.sql`（源端）和 `testcase/MySQL-columns-target.sql`（目标端）。

**前置条件**

- 两个可访问的 MySQL 实例（源端和目标端），建议使用独立端口隔离（如 3406 和 3408）
- `mysql` 客户端命令行工具已在 `PATH` 中
- 脚本默认会自动编译 `gt-checksum` 和 `repairDB` 二进制（需要 Go 环境）；若已编译可通过 `--skip-build` 跳过

**基本用法**

```bash
# 最简运行（自动编译 + 初始化 + 全测例）
bash scripts/regression-test-columns.sh \
    --src-port=3406 --dst-port=3408

# 跳过编译，直接使用已有二进制
bash scripts/regression-test-columns.sh \
    --src-port=3406 --dst-port=3408 --skip-build

# 仅预览测例列表，不实际执行
bash scripts/regression-test-columns.sh --dry-run

# 同时启用 Oracle stub 错误处理测例（TC-ORA-01）
bash scripts/regression-test-columns.sh \
    --src-port=3406 --dst-port=3408 --enable-oracle
```

**常用选项**

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `--src-port=PORT` | 必填 | 源端 MySQL 实例端口 |
| `--dst-port=PORT` | 必填 | 目标端 MySQL 实例端口 |
| `--host=IP` | `127.0.0.1` | 数据库主机地址 |
| `--user=USER` | `checksum` | 数据库用户名 |
| `--pass=PASS` | `checksum` | 数据库密码 |
| `--skip-init` | — | 跳过数据库初始化（fixture 已导入时可用） |
| `--skip-build` | — | 跳过二进制编译 |
| `--timeout=SEC` | `120` | 单用例超时秒数 |
| `--artifacts-dir=PATH` | 自动生成 | 自定义产物输出目录 |
| `--dry-run` | — | 仅打印测例列表，不执行 |
| `--enable-oracle` | — | 启用 Oracle stub 错误处理测例（TC-ORA-01） |

**测例说明**

| 用例 ID | 预期结果 | 场景 |
|---------|----------|------|
| TC-01-cols-basic-ignore | PASS | 非选中列（`ignored_col`）差异被正确忽略 |
| TC-02-cols-selected-diff-fix | PASS | 选中列（`amount`）差异经 repairDB 修复后收敛 |
| TC-03-cols-source-only-advisory | PASS-ADVISORY | source-only 行生成 `columns-advisory.*.sql`（全注释，无可执行 SQL） |
| TC-04-cols-simple-syntax | PASS | 简单列名语法（`columns=score`，无 schema.table 前缀） |
| TC-05-cols-cross-table-mapping | PASS | 跨表列名映射（`old_orders.src_total → new_orders.dst_total`）修复后收敛 |
| TC-06-cols-no-pk-ddl-yes | FAIL-EXPECTED | 无主键/唯一键表被标记为 `DDL-yes`（预期行为，不计失败） |
| TC-07-cols-target-only-extra | PASS | 目标端多余行 + `extraRowsSyncToSource=ON` 生成 DELETE 修复后收敛 |
| TC-08-cols-simple-multi-col | PASS | 简单语法多字段（`columns=score,note`）修复后收敛 |
| TC-ORA-01-cols-oracle-stub-error | ERROR-EXPECTED | Oracle 源端 stub 错误处理（需 `--enable-oracle`） |

**测试产物**

脚本运行结束后，产物保存在 `test-artifacts/columns-<日期时间>/` 目录下，包含：
- `results.csv`：所有测例的 ID、verdict、轮次、Diffs 摘要
- `report.txt`：格式化汇总报告
- `cases/<case_id>/`：各用例的配置文件、每轮输出、日志快照及 fixsql 文件

### `checkObject=struct` 的支持边界

当前 `checkObject=struct` 的能力边界如下：

1. `MySQL -> MySQL`
   - 已覆盖普通列、默认值、`charset/collation`、`PRIMARY KEY`、`UNIQUE`、普通索引（含前缀索引）、前缀索引、函数索引、虚拟列/生成列（STORED/VIRTUAL Generated Columns）、外键、`CHECK` 风险输出；
   - 已内置 `utf8 -> utf8mb3`、整数显示宽度、`ZEROFILL`、`ROW_FORMAT` 默认漂移、默认 `utf8mb4` 排序规则漂移等归一化规则；
   - `CHECK`、高风险外键不会自动执行高风险 DDL，而是保留为 `warn-only` 或 advisory 信息；
   - 当列宽度收窄（如 `VARCHAR(200)` → `VARCHAR(100)`）时，程序会自动检查目标端是否存在超宽数据行；若存在则输出 advisory SQL，不自动执行可能导致数据截断的 ALTER 操作。
2. `MariaDB -> MySQL 8.0/8.4`
   - 已覆盖安全子集：`JSON`、generated columns、`INET6`、`UUID`、`COMPRESSED`、`IGNORED INDEX`；
   - `MariaDB JSON` 可通过 `mariaDBJSONTargetType` 配置为 `JSON`、`LONGTEXT` 或 `TEXT`；
   - `COMPRESSED`、`MariaDB JSON -> LONGTEXT/TEXT` 的语义降级会保留为 `warn-only`；
   - **MariaDB 10.0 生成列兼容**：MariaDB 10.0 的 `INFORMATION_SCHEMA.COLUMNS.EXTRA` 对 STORED 生成列只返回 `PERSISTENT`，对 VIRTUAL 生成列只返回 `VIRTUAL`（均无 `GENERATED` 后缀），程序可自动识别并与 MySQL 8.0 的 `STORED GENERATED`/`VIRTUAL GENERATED` 等价比对；MariaDB 10.0 表达式的大写无反引号格式（如 `CAST(col AS SIGNED)`）与 MySQL 8.0 小写带反引号格式（如 `cast(\`col\` as signed)`）也会自动归一化，不产生误报。
3. `MariaDB -> MariaDB`（同系列或升级方向）
   - 支持所有 MySQL→MySQL 覆盖的常规列、索引、默认值、charset/collation 比对与 fix SQL 生成；
   - `COMPRESSED`、`PERSISTENT` 等 MariaDB 原生列属性在目标端保留，不会被剥除；
   - 当目标端为支持隐藏索引语法的 MariaDB 版本时，隐藏索引 fix SQL 使用 `IGNORED` 关键字，而非 MySQL 的 `INVISIBLE`；
   - 建议在回放前先审查 fix SQL，尤其关注隐藏索引、`DEFINER` 与跨版本 collation 相关语句。
4. `Oracle -> MySQL 8.0/8.4`
   - 目标端缺表时，自动从 Oracle 元数据生成 MySQL `CREATE TABLE` 语句，包含主键定义，适配 `sql_require_primary_key=ON`；
   - 列类型比对引入完整 Oracle→MySQL 类型映射层，对可接受的类型差异（如精度兼容）采用宽松判定，减少误报；映射关系如下：

     | Oracle 类型 | MySQL 映射类型 | 备注 |
     |---|---|---|
     | `VARCHAR2(n)` | `VARCHAR(n)` | |
     | `CHAR(n)` | `CHAR(n)` | |
     | `NCHAR(n)` | `CHAR(n)` | |
     | `NVARCHAR2(n)` | `VARCHAR(n)` | |
     | `NUMBER(p,s)` | `DECIMAL(p,s)` | |
     | `NUMBER(p)` | `TINYINT` / `SMALLINT` / `INT` / `BIGINT` | 按精度自动选择 |
     | `DATE` | `DATETIME` | |
     | `TIMESTAMP(n)` | `DATETIME(n)` | |
     | `FLOAT` | `DOUBLE` | |
     | `BINARY_FLOAT` | `FLOAT` | |
     | `BINARY_DOUBLE` | `DOUBLE` | |
     | `CLOB` / `NCLOB` | `LONGTEXT` | |
     | `BLOB` | `LONGBLOB` | |
     | `RAW(n)` | `VARBINARY(n)` | |
   - 列名仅大小写不同（Oracle 默认大写存储、MySQL 大小写不敏感）时不视为差异，不生成 `CHANGE COLUMN`；
   - `routine`/`trigger` 模式暂不支持 Oracle→MySQL 场景，启动阶段直接拒绝。
5. 以下对象当前只做识别、告警和 advisory 输出，不自动修复：
   - `SYSTEM VERSIONING`
   - `WITHOUT OVERLAPS`
   - `SEQUENCE`
6. **VIEW（视图）支持**（MySQL → MySQL 限定）：
   - `checkObject=struct` 会自动识别 `tables` 参数中的视图对象，并对视图定义进行比对；
   - 差异时 `Diffs=yes`，`ObjectType=view`；修复建议以 advisory 块形式写入 fixsql 文件，gt-checksum 本身不自动执行，DBA 手工审阅后可通过 repairDB 应用；
   - 差异来源分两层：① VIEW 定义（`SHOW CREATE VIEW`）不一致时，advisory 块中输出可执行的 `CREATE OR REPLACE VIEW` 建议 SQL（含前后字符集 SESSION 变量设置）；② 定义文本一致但列元数据（类型、nullable、charset、collation）存在漂移时，advisory 块标注 `suggested SQL: none`，**不提供可执行修复 SQL**（此类漂移通常源于底层基表结构变更，单纯重建视图无法修复）；
   - `DEFINER` 账号不计入差异判断（advisory 日志中仅做记录）；
   - `ALGORITHM=UNDEFINED` 与省略等价处理，不触发 `Diffs=yes`；`ALGORITHM=MERGE` / `TEMPTABLE` 等非默认值会保留在 advisory 建议 SQL 中；
   - **SQL SECURITY 差异不计入 `Diffs=yes`**：迁移时从 `DEFINER` 改为 `INVOKER` 属常见合理变更；程序仅在运行日志中输出 `Warn` 级别提示（含源/目标各自的值），方便 DBA 知晓但无需处理；advisory 建议 SQL 中会保留源端的 SQL SECURITY 设置，DBA 可在手工应用时自行调整；
   - **跨 schema 映射支持**：`tables=db1.*:db2.*` 场景下，部分 MySQL 版本的 `SHOW CREATE VIEW` 输出会在视图名前附加 schema 前缀（如 `` `db1`.`v1` ``），程序在归一化时会自动剥除该前缀，不产生误报；
   - **MariaDB 源端 collation 自动映射**：当源端为 MariaDB 11.5+（排序规则含 `uca1400`，如 `utf8mb4_uca1400_ai_ci`）时，advisory 块中的 `SET collation_connection` 语句会自动映射为 MySQL 等价排序规则（如 `utf8mb4_0900_ai_ci`），避免在 MySQL 8.0/8.4 上执行时报错；
   - `checkObject=data` 模式下视图会被自动跳过，不产生误报；
   - 仅支持 MySQL→MySQL，其他驱动组合（如 Oracle→MySQL）视图条目会被忽略并打印 Warn 日志；
   - **终端输出**：`checkObject=struct` 模式下，终端结果表格新增 `ObjectType` 列，可直观区分 `table` 行与 `view` 行；
   - **前置权限要求**：`SHOW CREATE VIEW` 依赖 `SHOW VIEW` 权限（MySQL 5.7+）；校验账号需对被检视图具备该权限，否则报 `Error 1142: SHOW VIEW command denied`。授权示例：`GRANT SELECT, SHOW VIEW ON db1.* TO 'checksum'@'%';`

### VIEW struct 校验配置示例与输出样本

以下示例对 `appdb.v_order_summary` 视图进行 MySQL→MySQL 结构一致性校验：

```ini
srcDSN = mysql|checksum:Checksum@3306@tcp(src-host:3306)/information_schema?charset=utf8mb4
dstDSN = mysql|checksum:Checksum@3306@tcp(dst-host:3307)/information_schema?charset=utf8mb4
tables = appdb.v_order_summary
checkObject = struct
datafix = file
fixFileDir = ./fixsql-view-check
```

执行后终端输出示例（定义一致，无差异）：

```text
Checksum Results Overview
Schema  Table            ObjectType  CheckObject  Diffs  Datafix
appdb   v_order_summary  view        struct       no     no
```

执行后终端输出示例（定义存在差异）：

```text
Checksum Results Overview
Schema  Table            ObjectType  CheckObject  Diffs  Datafix
appdb   v_order_summary  view        struct       yes    file
```

VIEW 定义差异时生成的 advisory fixsql（`./fixsql-view-check/appdb/v_order_summary.sql`）示例：

```sql
-- gt-checksum advisory begin: appdb.v_order_summary VIEW definition
-- generated as executable SQL; review before applying in the target session
-- level: advisory-only
-- kind: VIEW DEFINITION
-- reason: VIEW definition differs
SET character_set_client = utf8mb4;
SET collation_connection = utf8mb4_0900_ai_ci;
CREATE OR REPLACE SQL SECURITY DEFINER VIEW `appdb`.`v_order_summary` AS SELECT ...;
SET collation_connection = DEFAULT;
SET character_set_client = DEFAULT;
-- gt-checksum advisory end: appdb.v_order_summary VIEW definition
```

> **说明**：advisory 块内的 `SET`、`CREATE OR REPLACE VIEW` 为可执行 SQL，`repairDB` 会在执行 fixsql 文件时顺序执行这些语句。`SET character_set_client` / `SET collation_connection` 在 VIEW 重建前后成对设置和恢复，确保同一连接中后续对象的字符集上下文不受影响。当源端字符集信息不可用时，这两条 `SET` 语句不会输出。

列元数据漂移时的 advisory 输出（`suggested SQL: none`）：

```sql
-- gt-checksum advisory begin: appdb.v_order_summary VIEW definition
-- generated as executable SQL; review before applying in the target session
-- level: advisory-only
-- kind: VIEW COLUMN METADATA
-- reason: column metadata drift - column[0] differs: src="id|int|NO||" dst="id|bigint|NO||"
-- suggested SQL: none
-- gt-checksum advisory end: appdb.v_order_summary VIEW definition
```

> **注意**：列元数据漂移（列类型、nullable、charset 不一致）通常源于底层基表结构已变更，仅重建视图无法修复。此类场景 advisory 标注 `suggested SQL: none`，DBA 需先修复底层基表后再重新校验视图。

### `checkObject=routine` 和 `checkObject=trigger` 的比对机制

当 `checkObject` 设置为 `routine` 或 `trigger` 时，程序会分别比对源端与目标端的存储程序（`PROCEDURE`/`FUNCTION`）和触发器定义。当前版本在定义文本比对之外，还增加了以下 charset 元数据维度的比对：

1. **三维度 charset 元数据比对**：程序会从 `INFORMATION_SCHEMA.ROUTINES`（或 `TRIGGERS`）中提取 `CHARACTER_SET_CLIENT`、`COLLATION_CONNECTION`、`DATABASE_COLLATION` 三个元数据字段，逐一比对源端与目标端的值。任一维度不一致时，结果会显示为 `yes`。

2. **定义文本归一化**：程序在比对存储程序定义时，仅对 `CREATE PROCEDURE/FUNCTION` 头部标识符做大小写归一化，保留函数体内字符串字面量的原始大小写，避免因字面量大小写差异导致的误报。同时会剥离 MySQL 版本注释（`/*!...*/`）和整数显示宽度（如 `int(11)` → `int`）等平台差异。

3. **MariaDB→MariaDB 元数据比对**：当源端与目标端均为 MariaDB 时，`routine` 和 `trigger` 的三维度 charset 元数据比对（`CHARACTER_SET_CLIENT`、`COLLATION_CONNECTION`、`DATABASE_COLLATION`）自动启用；`COMMENT`/`DEFINER` 差异也会正常报告。对 `PROCEDURE`/`FUNCTION` 的 comment-only 差异，会生成 `ALTER PROCEDURE/FUNCTION ... COMMENT` 修复语句；对 `TRIGGER` 或包含定义文本差异的对象，则仍通过重建定义的 fix SQL 处理。

4. **collation 映射识别**：当源端为 `MariaDB 12.3+` 且使用 `uca1400` 系列排序规则时，程序会自动识别其与 `MySQL` 端 `uca0900` 排序规则的语义对应关系，结果显示为 `collation-mapped` 而非差异。

5. **查询容错**：当 charset 元数据查询失败时（如因权限不足），程序会输出 `Warn` 级别日志并跳过该维度的比对，而非静默返回空值导致误判。

### 结构迁移专项参数

| 参数名 | 可选值 | 默认值 | 说明 |
|---|---|---|---|
| `mariaDBJSONTargetType` | `JSON` / `LONGTEXT` / `TEXT` | `JSON` | 控制 `MariaDB JSON` alias 在 `MariaDB -> MySQL 8.0/8.4` 结构迁移时的目标列类型。`JSON` 语义最接近；`LONGTEXT` 适合作为兼容性保底；`TEXT` 当前已实现但未纳入发布级实库基线。 |
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
fixFileDir = ./fixsql-struct-mysql57-to80
```

以下示例表示执行 `MariaDB 10.6 -> MariaDB 10.11` 的数据校验：

```ini
srcDSN = mysql|checksum:Checksum@3306@tcp(src-mariadb106-host:3408)/information_schema?charset=utf8mb4
dstDSN = mysql|checksum:Checksum@3306@tcp(dst-mariadb1011-host:3409)/information_schema?charset=utf8mb4
tables = mydb.*
checkObject = data
datafix = file
```

以下示例表示执行 `MariaDB 10.6 -> MariaDB 10.11` 的表结构校验与 fix SQL 生成（含隐藏索引 `IGNORED` 适配）：

```ini
srcDSN = mysql|checksum:Checksum@3306@tcp(src-mariadb106-host:3408)/information_schema?charset=utf8mb4
dstDSN = mysql|checksum:Checksum@3306@tcp(dst-mariadb1011-host:3409)/information_schema?charset=utf8mb4
tables = mydb.*
checkObject = struct
datafix = file
fixFileDir = ./fixsql-struct-mariadb106-to1011
```

> **注意**：`MariaDB -> MariaDB` 场景下生成的隐藏索引修复语句使用 `IGNORED`（如 `ALTER TABLE ... ALTER INDEX idx IGNORED`），与 MySQL 的 `INVISIBLE` 语法不同，请勿将 fix SQL 跨平台回放。
>
> **补充说明**：如果目标端 MariaDB 版本本身不具备隐藏索引语法能力，则不会存在可直接回放的隐藏索引修复语句；此类差异建议先在测试环境完成验证，再决定是否人工调整。

以下示例表示执行 `MariaDB 10.5 -> MySQL 8.0` 的安全子集表结构校验与 fix SQL 生成，并将 `JSON` alias 降级为 `LONGTEXT`：

```ini
srcDSN = mysql|checksum:Checksum@3306@tcp(src-mariadb105-host:3407)/information_schema?charset=utf8mb4
dstDSN = mysql|checksum:Checksum@3306@tcp(dst-mysql80-host:3406)/information_schema?charset=utf8mb4
tables = gt_phase1_mariadb105.*
checkObject = struct
datafix = file
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
5. 列宽度收窄（Column Width Shrink）时目标端存在超宽数据或安全检查查询异常

对于 VIEW 对象，以下情形结果显示为 `Diffs=yes` 但 advisory 标注 `suggested SQL: none`，需要 DBA 手工处理：

6. VIEW 定义文本一致，但底层列元数据（类型、nullable、charset、collation）已漂移——通常意味着底层基表结构已变更，需先修复基表后 VIEW 才能恢复一致；
7. SQL SECURITY 差异**不在此列**：此类差异仅记录 Warn 日志，不标记 `Diffs=yes`，无需额外处理（DBA 可从日志中确认差异值后决定是否手工统一）。

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

当源端为 `MariaDB` 时（适用于 `MariaDB -> MySQL 8.0/8.4` 和 `MariaDB -> MariaDB` 两类场景），程序的权限检查行为如下：

1. 源端 `MariaDB`：跳过全局权限预检查，不再要求 `SESSION_VARIABLES_ADMIN`、`REPLICATION CLIENT` 这类 `MySQL` 命名的全局权限；
2. 目标端为 `MySQL 8.0/8.4`：继续按现有逻辑检查全局权限；目标端为 `MariaDB`：同样跳过全局权限预检查；
3. 源端与目标端表级权限：`checkObject=data` 仍按既有逻辑检查 `SELECT` 以及 `datafix=table` 时所需的对象权限；`checkObject=struct` 则需确保可读取相关元数据并具备目标端执行 fix SQL 所需的对象级 DDL 权限；
4. 如果终端提示 `Missing required global privileges`，请优先打开 debug 日志，根据日志中源端/目标端各自的权限检查结果确认具体缺失项，而不要仅凭终端输出中的概括性提示判断。

## 结果文件导出

`gt-checksum` v1.3.0 新增统一的校验结果 CSV 导出能力（#I6KMQF）。

### 功能概述

- 每次运行结束后自动生成一个结果 CSV 文件，默认命名为 `gt-checksum-result-<RunID>.csv`（`RunID` 格式 `YYYYMMDDHHmmss`，精度为秒级；同一秒内多次启动会产生相同 RunID）。
- CSV 文件使用 **UTF-8 BOM** 编码，可被 Excel 直接打开，无需额外配置。
- CSV 列头固定，包含全部 14 列，适用于 `data`、`struct`、`routine`、`trigger` 四种模式，不使用的列留空而不是省略。
- CSV 始终包含**完整结果**，不受 `terminalResultMode` 过滤影响。

### CSV 列头说明

| 列名 | 说明 |
|------|------|
| `RunID` | 本次运行标识（`YYYYMMDDHHmmss`，精度秒级） |
| `CheckTime` | 结果导出时间（`YYYY-MM-DD HH:MM:SS`） |
| `CheckObject` | 用户请求的校验模式：`data` / `struct` / `routine` / `trigger`；`routine` 模式下存储过程和函数均统一显示为 `routine`，具体类型见 `ObjectType` |
| `Schema` | 对象所在 schema |
| `Table` | 表名；非表对象（view / routine / trigger）时为空 |
| `ObjectName` | 统一对象名（表名、视图名、存储过程名、函数名、触发器名） |
| `ObjectType` | `table` / `view` / `procedure` / `function` / `trigger` / `sequence` |
| `IndexColumn` | 仅 `data` 模式使用，显示校验所用索引列 |
| `Rows` | 行数（`DDL-yes` 时为空） |
| `Diffs` | 差异状态：`yes` / `no` / `DDL-yes` / `warn-only` / `collation-mapped` |
| `Datafix` | 修复方式：`file` / `table` |
| `Mapping` | schema/table 映射说明（无映射时为空） |
| `Definer` | routine / trigger 场景下的 DEFINER |
| `Columns` | 仅在 `columns` 子集校验模式下非空，显示本次实际参与比对的列计划；全列模式下为空 |

### 结果导出相关参数

| 参数 | 默认值 | 可选值 | 说明 |
|------|--------|--------|------|
| `resultExport` | `csv` | `OFF` / `csv` | 是否导出 CSV；`OFF` 时不生成文件 |
| `resultFile` | `result` | 任意路径字符串 | 自定义导出路径；未设置时自动生成 `result/gt-checksum-result-<RunID>.csv` |
| `terminalResultMode` | `all` | `all` / `abnormal` | 终端显示模式；`abnormal` 只显示差异行，不影响 CSV 内容 |

以上参数均支持 CLI 覆盖，高于配置文件：

```bash
# 禁用 CSV 导出
gt-checksum -c gc.conf --resultExport OFF

# 自定义 CSV 路径
gt-checksum -c gc.conf --resultFile ./output/result.csv

# 终端只显示差异行
gt-checksum -c gc.conf --terminalResultMode abnormal
```

### 使用示例

执行数据校验并查看 CSV 结果：

```bash
$ gt-checksum -c gc.conf

...
Result exported to: result/gt-checksum-result-20260323195530.csv
```

```bash
# CSV 文件含 UTF-8 BOM，直接用 cat/head 输出会在首行前显示 BOM 字符。
# 推荐用 python 或 Excel 查看，也可用 tail -n +1 跳过 BOM：
$ python3 -c "
import csv, sys
with open('result/gt-checksum-result-20260323195530.csv', encoding='utf-8-sig') as f:
    for row in csv.reader(f): print(row)
" | head -3
['RunID', 'CheckTime', 'CheckObject', 'Schema', 'Table', 'ObjectName', 'ObjectType', 'IndexColumn', 'Rows', 'Diffs', 'Datafix', 'Mapping', 'Definer', 'Columns']
['20260323195530', '2026-03-23 19:55:31', 'data', 'sbtest', 'sbtest1', 'sbtest1', 'table', 'id', '10000', 'no', 'file', '', '', '']
['20260323195530', '2026-03-23 19:55:31', 'data', 'sbtest', 'sbtest2', 'sbtest2', 'table', 'id', '4999', 'yes', 'file', '', '', '']
```

终端只显示有差异的行：

```bash
$ gt-checksum -c gc.conf --terminalResultMode abnormal

** gt-checksum Overview of results **
Schema  Table    IndexColumn  CheckObject  Rows  Diffs  Datafix
sbtest  sbtest2  id           data         4999  yes    file

Result exported to: result/gt-checksum-result-20260323195530.csv
```

> **注意**：
> - `resultExport=OFF` 时不生成 CSV 文件，行为与 v1.2.x 一致。
> - `resultFile` 未配置时默认输出到 `result/` 目录；指定自定义路径时，如果父目录不存在会自动创建（v1.3.0 起）。
> - CSV 导出失败（如无写权限）时只输出 Warning，不影响校验主流程的退出码。

---

## 配置参数详解

**gt-checksum** 支持命令行参数与配置文件方式运行。大多数参数通过配置文件指定；部分高频参数支持 CLI 覆盖（优先级高于配置文件），包括 `--showActualRows`、`--resultExport`、`--resultFile`、`--terminalResultMode`。

配置文件中所有参数的详解可参考模板文件 [gc-sample.conf](./gc-sample.conf)。

**gt-checksum** 命令行参数选项详细解释如下。

- `-c / -f`。类型：**string**，默认值：**空**。作用：指定配置文件名。

  使用案例：
  ```bash
  $ gt-checksum -c ./gc.conf
  ```

- `--showActualRows`。类型：**string**，可选值：`ON` / `OFF`。作用：覆盖配置文件中的 `showActualRows` 参数。

- `--resultExport`。类型：**string**，可选值：`OFF` / `csv`。作用：覆盖配置文件中的 `resultExport` 参数，控制是否生成 CSV 结果文件。

- `--resultFile`。类型：**string**。作用：覆盖配置文件中的 `resultFile` 参数，指定 CSV 输出文件路径。

- `--terminalResultMode`。类型：**string**，可选值：`all` / `abnormal`。作用：覆盖配置文件中的 `terminalResultMode` 参数，控制终端结果显示模式。

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

**repairDB** 工具用于执行 SQL 修复文件，支持批量并行执行 SQL 文件并自动处理事务。内置六阶段对象类型调度模型，按对象依赖顺序分阶段执行，确保修复结果的正确性。针对并行修复过程中可能出现的死锁冲突（MySQL Error 1213），内置自动重试机制：按 `BEGIN ... COMMIT` 事务块进行重试，最多 3 次。

#### 六阶段调度模型

repairDB 根据 SQL 文件的命名前缀自动识别对象类型，并按以下固定顺序分阶段执行：

| 阶段 | 文件识别规则 | 阶段内排序 | 说明 |
|------|-------------|-----------|------|
| DELETE | 文件名包含 `-DELETE-` 模式 | 稳定排序 | 优先执行删除操作，为后续修复清理旧数据 |
| TABLE | 文件名以 `table.` 开头 | 随机 shuffle | 打散锁热点，降低并发写同一表的概率 |
| VIEW | 文件名以 `view.` 开头 | 稳定排序 | 视图依赖 TABLE，须在 TABLE 之后执行 |
| ROUTINE | 文件名以 `routine.` 开头 | 稳定排序 | 存储过程/函数，在 VIEW 之后执行 |
| TRIGGER | 文件名以 `trigger.` 开头 | 稳定排序 | 触发器依赖基表与 ROUTINE，最后重建 |
| UNKNOWN | 无法识别前缀的文件 | 稳定排序 | 兼容手工 SQL 文件；打印 Warn 日志，最后执行 |

**阶段间保持硬屏障**：每个阶段全部文件执行完成后，下一阶段才会启动。前序阶段出现任何失败，后续阶段不再启动，整体以非零退出码退出。

**每阶段独立连接池**：各阶段分别打开并关闭数据库连接池，防止 `FOREIGN_KEY_CHECKS`、`UNIQUE_CHECKS` 等 session 变量通过连接复用在阶段间泄漏。

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
| fixFileDir | string | fixsql | 存放修复SQL文件的目录 |
| logbin | string | ON | 控制修复时是否写入 binlog；`OFF` 时每条连接执行 `SET sql_log_bin=0`，需要 SUPER 或 SESSION_VARIABLES_ADMIN 权限 |

### 执行流程

1. 读取配置文件或命令行参数（命令行参数只能指定 fixsql 所在目录，不支持指定其他参数）；
2. 扫描指定目录下的所有 `.sql` 文件，并按对象类型分为六个阶段（DELETE / TABLE / VIEW / ROUTINE / TRIGGER / UNKNOWN）；
3. 打印各阶段文件数量汇总；若存在 UNKNOWN 文件，额外打印 Warn 日志；
4. 按 DELETE→TABLE→VIEW→ROUTINE→TRIGGER→UNKNOWN 顺序逐阶段执行；每个阶段单独建立连接池、执行完成后关闭；
5. 每阶段内以 `parallelThds` 线程并发执行该阶段所有文件；
6. 将 SQL 文件拆分为执行单元：普通语句单独执行，`BEGIN ... COMMIT/ROLLBACK` 作为一个事务块执行；
7. 若检测到死锁错误（Error 1213），仅对当前失败事务块自动重试，最多 3 次（指数退避），不重试整个 SQL 文件；
8. 某阶段任一文件失败则该阶段报错退出，后续阶段不再启动；
9. 全部阶段完成后输出总耗时。

### 注意事项

1. **数据库权限**：执行 **repairDB** 的数据库账户需要具备执行SQL文件中包含的SQL语句的权限。

2. **SQL文件格式**：SQL文件可以包含多行SQL命令，会自动按分号分割并逐个执行。

   对于包含 `PROCEDURE`、`FUNCTION`、`TRIGGER` 定义的 fixSQL，`repairDB` 也支持解析 `DELIMITER` 语法并执行。但如果脚本中包含 `DROP + CREATE PROCEDURE/FUNCTION/TRIGGER`，则目标库仍需预先满足对应 `DEFINER` 账号与权限要求。

3. **事务管理**：按SQL执行单元处理事务。`BEGIN ... COMMIT` 内的语句作为一个事务块执行，失败时回滚该事务块；普通语句按语句级执行。

4. **执行顺序**：按六阶段固定顺序执行：DELETE→TABLE→VIEW→ROUTINE→TRIGGER→UNKNOWN。DELETE 阶段优先清理旧数据；TABLE 阶段采用随机 shuffle 降低同表锁争用；VIEW/ROUTINE/TRIGGER/UNKNOWN 阶段使用稳定排序以保证审计可读性和确定性回放。无法识别前缀的文件进入 UNKNOWN 阶段最后执行，并打印 Warn 提示。

5. **错误处理**：遇到死锁错误（Error 1213）时会自动重试当前失败事务块，最多 3 次；若仍失败或遇到非死锁错误，则停止并输出错误信息。

6. **目录存在性**：检查指定的 `fixFileDir` 目录是否存在，如果不存在则报错退出。

7. **并行执行**：以 `parallelThds` 线程数并行执行SQL文件。为了提高数据修复效率，可以考虑临时加大 `parallelThds` 参数值，但同时需要注意对目标数据库的负载的影响和死锁概率。

8. **DEFINER 前置检查（可选但强烈建议）**：当 fixSQL 中包含 `CREATE DEFINER=... PROCEDURE/FUNCTION/TRIGGER` 时，建议在执行前先扫描 fixSQL 并核对目标库账号体系。例如先确认脚本里引用了哪些 `DEFINER`，再确认目标库是否已存在对应账号及授权。若缺失账号或权限，应先由 DBA 补齐，再执行 `repairDB`。

### 示例输出

程序执行过程中的输出会同时打印到标准输出和 `repairDB.log` 文件中，示例如下：

```bash
$ ./repairDB ./myfixsql

2026/01/29 10:00:00 Configuration information:
2026/01/29 10:00:00   DstDSN: mysql|checksum:Checksum@3306@tcp(127.0.0.1:3306)/sbtest?charset=utf8mb4
2026/01/29 10:00:00   ParallelThds: 4
2026/01/29 10:00:00   FixFileDir: ./myfixsql
2026/01/29 10:00:00   LogFile: repairDB.log
2026/01/29 10:00:00 Stage classification: DELETE=2 TABLE=3 VIEW=1 ROUTINE=0 TRIGGER=0 UNKNOWN=0
2026/01/29 10:00:00 [DELETE] planned execution order (2 files):
2026/01/29 10:00:00 [DELETE] #1 table.sbtest.t1-DELETE-1.sql
2026/01/29 10:00:00 [DELETE] #2 table.sbtest.t2-DELETE-1.sql
2026/01/29 10:00:00 [DELETE] starting execution (2 files), concurrency: 4
2026/01/29 10:00:00 [DELETE] execution sequence #1: ./myfixsql/table.sbtest.t1-DELETE-1.sql
2026/01/29 10:00:00 [DELETE] execution sequence #2: ./myfixsql/table.sbtest.t2-DELETE-1.sql
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/table.sbtest.t1-DELETE-1.sql, time taken: 10ms
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/table.sbtest.t2-DELETE-1.sql, time taken: 8ms
2026/01/29 10:00:00 [DELETE] execution completed
2026/01/29 10:00:00 [TABLE] planned execution order (3 files):
2026/01/29 10:00:00 [TABLE] #1 table.sbtest.t2-1.sql
2026/01/29 10:00:00 [TABLE] #2 table.sbtest.t1-1.sql
2026/01/29 10:00:00 [TABLE] #3 table.sbtest.t3-1.sql
2026/01/29 10:00:00 [TABLE] starting execution (3 files), concurrency: 4
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/table.sbtest.t2-1.sql, time taken: 10ms
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/table.sbtest.t1-1.sql, time taken: 12ms
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/table.sbtest.t3-1.sql, time taken: 9ms
2026/01/29 10:00:00 [TABLE] execution completed
2026/01/29 10:00:00 [VIEW] starting execution (1 files), concurrency: 4
2026/01/29 10:00:00 Successfully executed SQL file ./myfixsql/view.sbtest.v_order-1.sql, time taken: 5ms
2026/01/29 10:00:00 [VIEW] execution completed
2026/01/29 10:00:00 All SQL files execution completed, total time taken: 0m0.058s
2026/01/29 10:00:00 repairDB executed successfully
```

> **说明**：TABLE 阶段的执行顺序为随机 shuffle（如上例中 t2 先于 t1 执行），其余阶段按文件名稳定排序。若目录中存在无法识别前缀的文件，会在分类汇总后打印 `[WARN]` 提示。

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

截止当前的 v1.4.0 版本，已知存在以下几个约束/问题。

- 当存在触发器时，因为触发器的作用，可能导致在修复完一个表后，触发其他表被改变，从而看起来像是修复后仍不一致的情况。这种情况下，需要先临时删除触发器进行修复，完成后在重新创建触发器。

- 当表校验结果仅存在partition定义不一致时，报告Diffs=yes，但生成的fixSQL中的SQL语句是被注释的，不会被repairDB执行，需要DBA手动调整修复，避免误操作导致数据丢失。在 Oracle→MySQL 场景下，分区比对行为有所不同：仅做存在性比对并输出 advisory 告警，不生成任何分区修复 SQL，原因是 Oracle 与 MySQL 的分区语法差异过大，不适合自动转换；DBA 需根据 advisory 输出手动在目标端重建分区定义。

- 为了安全起见，当设置checkObject=data之外的其他值时，即便同时设置datafix=table，也不会直接在线完成修复，需要改成datafix=file，生成fix SQL后再由DBA手动完成。

- 当设置 `checkObject=trigger` 或 `routine` 时，如果连接数据库的账号没有相应的权限而无法读取到元数据，会导致检查结果不完整。当前版本在 charset 元数据查询失败时会输出 `Warn` 级别日志，但仍需先授予相应权限才能确保结果准确。

- 当 `checkObject=trigger` 或 `routine` 生成的 fixSQL 中包含 `DROP + CREATE PROCEDURE/FUNCTION/TRIGGER` 时，目标库必须预先存在源端定义中的 `DEFINER` 账号及权限，否则执行会失败。这是环境约束，不是程序实现错误。建议在执行 `repairDB` 前，先对 fixSQL 中的 `DEFINER` 做一次人工检查。

- 已支持 `MySQL 5.6`、`5.7`、`8.0`、`8.4` 的同版本和升级链路校验；但仍不支持 `src > dst` 的 downgrade 场景，程序会在启动阶段直接退出。

- `MariaDB JSON -> TEXT` 虽已具备规则改写和单测覆盖，但当前尚未纳入发布级实库基线；如需使用，建议先在测试环境中自行完成 fix SQL 回放与二次 compare 验证。

- 对 MyISAM、MEMORY 等不支持 MVCC 的引擎，在源端或目标端存在并发写入时，数据校验结果的一致性无法保证（这些引擎无法提供一致性快照）；若相关表处于只读或静态状态，校验可正常执行。

- 在 Oracle→MySQL `struct` 模式下，以下 Oracle 类型的映射存在不可逆的语义损失，程序会输出 `WarnOnly` 级别提示但不阻止比对，DBA 应在执行 fix SQL 前人工确认：`TIMESTAMP WITH [LOCAL] TIME ZONE` 映射为 MySQL `datetime`，时区信息将丢失；`INTERVAL YEAR TO MONTH` 和 `INTERVAL DAY TO SECOND` 映射为 `varchar(30)`，仅保留字符串形式，区间计算语义完全丧失。建议在迁移完成后，对含上述类型的列进行业务层验证。

## 问题反馈

可以 [提交issue](https://gitee.com/GreatSQL/gt-checksum/issues) 查看或提交 gt-checksum 相关bug。
