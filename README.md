[![](https://img.shields.io/badge/GreatSQL-官网-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-论坛-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-博客-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-Apache_v2.0-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/blob/master/LICENSE)
[![](https://img.shields.io/badge/release-4.0.2-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/releases)

# gt-checksum
**gt-checksum** 是GreatSQL社区开源的数据库校验及修复工具，支持 MySQL-family（MySQL/Percona/GreatSQL/MariaDB等）、Oracle 等主流数据库。

## 简介

MySQL DBA 经常使用 **pt-table-checksum** 和 **pt-table-sync** 进行数据校验及修复，但这类工具在 MySQL MGR、高可用多节点、企业上云下云、跨版本升级、MariaDB/MySQL 兼容迁移、Oracle -> MySQL 异构迁移等场景下存在覆盖不足或使用成本较高的问题。

**gt-checksum** 是 GreatSQL 社区开源的数据库校验及修复工具，面向 MySQL-family（MySQL/Percona/GreatSQL/MariaDB 等）和 Oracle 等数据库，支持数据、表结构、存储程序、触发器等对象的差异校验，并可按场景生成修复 SQL 或执行在线修复。

当前版本进一步增强了长任务可恢复性与修复可审计能力，支持断点续传、反向回滚 SQL、自定义数据类型映射、SSL 加密连接、CSV 结果导出以及结构修复辅助参数等能力，适合用于迁移验收、升级校验、主从/组复制一致性检查和定期巡检等场景。

## v4.0.2 关键变化

- **[功能新增]** 新增 `hashAlgorithm` 参数，支持选择 `xxhash64`（默认）或 `md5` 哈希算法
- **[性能优化]** 引入 XXHash64 算法，数据校验性能提升约 15-25 倍
- **[问题修复]** 修复 table 模式下多行 INSERT 修复 SQL 写入文件后读回错乱的问题
- **[测试完善]** 新增 checksum 模块单元测试

更多详细变化详见 [CHANGELOG](./CHANGELOG.md)。

**gt-checksum** 支持以下几种常见业务需求场景：
1. **MySQL 主从复制 / 高可用一致性校验**：当主从复制中断、延迟过大或节点异常恢复后，可使用 **gt-checksum** 快速校验源端与目标端数据差异，并按需生成修复 SQL 或执行在线修复。
2. **MySQL MGR / 多节点组复制一致性校验**：在 MGR 节点异常退出、重新加入或切换前，可选择一个节点作为基准，对其他节点执行数据一致性校验，降低直接重建节点的时间成本。
3. **企业上云、下云及跨机房迁移验收**：在数据迁移完成后，可对数据、表结构、存储程序、触发器等对象进行一致性校验，发现字符集、排序规则、数据类型或对象定义差异；需要加密链路时可配置源端和目标端 SSL 连接。
4. **MySQL-family 跨版本升级迁移**：适用于 MySQL/GreatSQL/Percona 等同版本主线或升级方向的数据校验与对象校验，例如 `5.6 -> 5.7`、`5.7 -> 8.0`、`8.0 -> 8.4` 等场景，支持 `data`、`struct`、`routine`、`trigger` 四种 `checkObject` 模式。
5. **Oracle -> MySQL 8.0/8.4 异构迁移**：支持数据校验与表结构校验，可处理常见 Oracle 与 MySQL 数据类型、字符、数值精度、主键、外键等差异；当前 `routine` / `trigger` 模式暂不支持 Oracle -> MySQL 场景。
6. **MariaDB -> MySQL 8.0/8.4 迁移**：适用于 `MariaDB 10.x+ -> MySQL 8.0/8.4`，支持 `data`、`struct`、`routine`、`trigger` 四种模式；其中结构校验覆盖安全子集，并支持通过 `dTypeMappingFile` 自定义数据类型映射规则。
7. **MariaDB 实例间升级校验**：适用于 MariaDB 同系列或升级方向迁移，支持 `data`、`struct`、`routine`、`trigger` 四种模式；支持的系列包括 `10.0`、`10.1`、`10.2`、`10.3`、`10.4`、`10.5`、`10.6`、`10.11`、`11.4`、`11.5`、`12.3`，不支持 downgrade。
8. **长时间大表校验与可审计修复**：对于大表或长时间运行任务，可启用断点续传避免异常退出后从头开始；在数据修复场景下可生成反向回滚 SQL，便于修复前审计和修复后快速回退；结构修复场景下可按需使用 `truncateBeforeAlter` 辅助目标端数据可丢弃的大表结构调整。

## 版本策略

gt-checksum 采用**滚动发布**策略，官方仅维护最新发布版本。

**推荐做法：** 始终使用最新版本，以获得完整的功能支持和问题修复。

**旧版本说明：**
- 旧版本不再接受新功能开发
- 若旧版本存在以下类型的 bug，且对应功能在最新版本中仍然保留，则该 bug 将在未来版本中一并修复：
  - 安全漏洞
  - 数据正确性问题（如校验结果错误、修复 SQL 生成有误等）
- 性能优化、兼容性新增等不在旧版 bug fix 范围内

**不修复的情形：**
- 对应功能已在新版本中移除或重构
- 问题属于性能、兼容性或行为变更类

## 下载

可以 [这里](https://gitee.com/GreatSQL/gt-checksum/releases) 下载预编译好的二进制文件包，已经在 Ubuntu、CentOS、RHEL 等多个系统环境下测试通过。

如果需要校验Oracle数据库，则还需要先下载Oracle数据库相应版本的驱动程序，并配置驱动程序使之生效。例如：待校验的数据库为Oracle 11-2，则要下载Oracle 11-2的驱动程序，并使之生效，否则连接Oracle会报错。详细方法请见下方内容：[**下载配置Oracle驱动程序**](./gt-checksum-manual.md#下载配置Oracle驱动程序)。

## 快速运行

- 不带任何参数

```bash
$  gt-checksum
No config file specified and there is no gc.conf in the current directory, run the command with -h or --help
```

如果当前目录下有配置文件*gc.conf*，则会读取该配置文件开始运行，例如：

```bash
$ gt-checksum

gt-checksum: Automatically loading configuration file 'gc.conf' from current directory.

gt-checksum is initializing
gt-checksum is reading configuration files 
```

- 查看版本号

```bash
$  gt-checksum -v
gt-checksum version 4.0.2
```

- 查看使用帮助

```bash
$  gt-checksum -h
NAME:
   gt-checksum - opensource database checksum and sync tool by GreatSQL

USAGE:
   gt-checksum [global options] command [command options] [arguments...]
```

- 指定配置文件方式，执行数据校验

拷贝或重命名模板文件*gc-sample.conf*为*gc.conf*，主要修改`srcDSN`,`dstDSN`,`tables`,`ignoreTables`等几个参数后，执行如下命令进行数据校验：

```bash
$  gt-checksum -c ./gc.conf

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

> 开始执行数据校验前，要先在源和目标数据库创建相应的专属账号并授权。更多详情见手册中的 [**数据库授权**](./gt-checksum-manual.md#数据库授权) 章节。

- 极简配置下分别运行四种校验模式

只需 `srcDSN`、`dstDSN`、`tables` 三个参数即可启动校验，通过 `checkObject` 切换校验对象类型：

**data 模式**（默认）：校验表数据差异，生成修复 SQL 文件。

```ini
checkObject=data
```

执行后输出示例：

```text
Checksum Results Overview
Schema  Table  IndexColumn  CheckObject  Rows        Diffs  Datafix
mydb    t1     id           data         1000,1000   yes    file
```

**struct 模式**：校验表结构（列、索引、分区、外键等）差异，生成结构修复 SQL。`checkObject=struct` 还支持 MySQL→MySQL 场景下的 **VIEW（视图）** 校验，自动识别视图对象并比对定义，差异时以 advisory 块形式输出修复建议。

```ini
checkObject=struct
```

执行后输出示例：

```text
Checksum Results Overview
Schema  Table  ObjectType  CheckObject  Diffs  Datafix
mydb    t1     table       struct       yes    file
mydb    t2     table       struct       no     no
```

**routine 模式**：校验存储过程和函数定义差异（含 charset 元数据三维度比对）。

```ini
checkObject=routine
```

执行后输出示例：

```text
Checksum Results Overview
Schema  RoutineName  CheckObject  Diffs  Datafix
mydb    MYADD        routine      yes    file
mydb    P1           routine      no     no
```

**trigger 模式**：校验触发器定义差异（含 charset 元数据三维度比对）。

```ini
checkObject=trigger
```

执行后输出示例：

```text
Checksum Results Overview
Schema  TriggerName    CheckObject  Diffs  Datafix
mydb    trg_after_ins  trigger      yes    file
mydb    trg_before_upd trigger      no     no
```

> **说明**：
> - `data` 模式会根据 `datafix` 参数生成修复 SQL 文件（`file`）或执行在线修复（`table`）。
> - `struct`、`routine`、`trigger` 模式即使设置 `datafix=table`，也会强制导出 fix SQL 文件供 DBA 审查后再手动执行。
> - `routine` / `trigger` 生成的 fixSQL 通常包含 `DROP + CREATE` 语句，执行前需确认目标库中 `DEFINER` 账号与权限满足要求。
> - 更多配置和输出示例见 [**快速使用案例**](./gt-checksum-manual.md#快速使用案例)。

### 连接串密码加密

v4.0.0 起，配置文件中的 `srcDSN` / `dstDSN` password 不再允许明文，必须先使用 `gt-dsn-crypt` 生成 `ENC[...]` 密文，并在启动时提供 key：

```bash
KEY=$(gt-dsn-crypt gen-key)
printf '%s' '数据库密码' > ./password.txt
GT_CHECKSUM_DSN_KEY="$KEY" gt-dsn-crypt encrypt --password-file ./password.txt
GT_CHECKSUM_DSN_KEY="$KEY" gt-checksum -c ./gc.conf
```

配置示例：

```ini
srcDSN=mysql|checksum:ENC[v1:aes256gcm:default:...:...]@tcp(src-host:3306)/information_schema?charset=utf8mb4
dstDSN=mysql|checksum:ENC[v1:aes256gcm:default:...:...]@tcp(dst-host:3306)/information_schema?charset=utf8mb4
```

`--key` 优先级高于 `GT_CHECKSUM_DSN_KEY`，`repairDB` 也支持相同的 `--key` / 环境变量方式；密钥文件不受支持。错误日志和 debug 日志会自动脱敏 DSN，避免解密后的 password 再次泄露。

每次校验结束后，默认会在 `result/` 目录下自动生成结果 CSV 文件（默认开启），例如：`result/gt-checksum-result-20260323195530.csv`。如果配置了 `resultFile`，则以配置路径为准。使用 Excel 或命令行可直接查看完整校验结果：

```bash
$ cat result/gt-checksum-result-20260323195530.csv

RunID,CheckTime,CheckObject,Schema,Table,ObjectName,ObjectType,IndexColumn,Rows,Diffs,Datafix,Fixed,Mapping,Definer,Columns
20260323195530,2026-03-23 19:55:31,data,sbtest,sbtest2,sbtest2,table,id,4999,yes,file,,,,
```

全列校验时最后一列 `Columns` 为空；当启用 `columns` 子集校验时，这一列会显示本次实际参与比对的列计划。`Fixed` 用于展示 `datafix=table` 在线修复 SQL 的执行状态：无差异且两端行数一致时为 `skipped`，修复 SQL 无报错时为 `yes`，任一修复执行错误时为 `no`；非适用场景为空。

如需只在终端显示差异行，可配置 `terminalResultMode=abnormal`（CSV 仍输出完整结果）：

```bash
$ gt-checksum -c ./gc.conf --terminalResultMode abnormal
```

查看运行目录下是否生成修复SQL文件目录，例如：fixsql

执行 repairDB 工具进行数据修复并查看执行结果：

```bash
$ ./repairDB ./fixsql && cat ./repairDB.log

...
2026/01/29 15:45:22 Stage classification: DELETE=1 TABLE=3 VIEW=1 ROUTINE=0 TRIGGER=0 UNKNOWN=0
2026/01/29 15:45:22 [DELETE] starting execution (1 files), concurrency: 4
2026/01/29 15:45:22 [DELETE] execution completed
2026/01/29 15:45:22 [TABLE] starting execution (3 files), concurrency: 4
2026/01/29 15:45:22 Successfully executed SQL file ... time taken: 605.002µs
2026/01/29 15:45:22 [TABLE] execution completed
2026/01/29 15:45:22 [VIEW] execution completed
2026/01/29 15:45:22 All SQL files execution completed, total time taken: 0m0.012s
2026/01/29 15:45:22 repairDB executed successfully
```
这就表示完成修复，可以再次执行数据校验，确认数据一致性。

如果启用了 `genRollSQL=ON` 或指定表名列表，程序会把反向回滚 SQL 写入 `rollFileDir`（默认 `rollsql`）。该能力适用于 `checkObject=data` 下的 `datafix=file` 和 `datafix=table`：前者在生成 fixsql 的同时生成 rollsql，后者在在线修复目标端的同时生成 rollsql。需要撤销本次修复时，可在确认目标端无业务并发写入且 rollback SQL 与实际修复范围一致后执行：

```bash
$ ./repairDB ./rollsql
```

**注意**：由于是并行执行数据修复工作，修复过程中可能产生事务死锁冲突。`repairDB` 在检测到 MySQL deadlock（Error 1213）时，会自动对当前失败的事务块（`BEGIN ... COMMIT`）执行重试，最多重试 3 次；而不会重试整个 SQL 文件，从而降低主键重复冲突风险。建议修复结束后检查 `repairDB.log`：若死锁在 3 次重试内已恢复，可直接再次执行校验；若仍有未恢复死锁或其他错误，再手动处理对应 SQL 文件。

**重复键处理**：当 TABLE 阶段的 multi-values INSERT 因 MySQL `Error 1062: Duplicate entry` 失败时，`repairDB` 会定位对应 SQL 文件和行号，在内存中拆成多条 single-value INSERT 逐条重试；仍然重复的行会记录 `[DUPKEY-SPLIT]` 日志并跳过，其他行继续执行，原 SQL 文件不会被改写。

**中断处理**：修复执行中收到 `Ctrl+C` 或 `SIGTERM` 时，`repairDB` 会停止调度新的 SQL 文件，并等待已开始执行的文件完成；启用 `resume=ON/ASK` 后，下次执行会跳过已成功文件并继续剩余文件。

### repairDB CLI 参数

| 参数 | 说明 |
|---|---|
| `-conf` | 配置文件路径（默认 `gc.conf`） |
| `-f` / `--force` | 跳过交互式确认，直接执行修复 |
| `--dry-run` | 仅展示预执行统计报告，不执行实际修复 |
| `--result-file` | 自定义 CSV 报告输出路径（默认 `result/repairDB-result-<timestamp>.csv`） |
| `--key` | base64 编码 32 字节 key，用于解密 `dstDSN` 中的 `ENC[...]` password；优先级高于 `GT_CHECKSUM_DSN_KEY` |

### repairDB 配置文件参数

| 参数 | 说明 | 示例 |
|---|---|---|
| `dstDSN` | 目标数据库连接串，password 必须为 `ENC[...]` | `mysql|user:ENC[...]@tcp(host:3306)/db` |
| `parallelThds` | 并发执行线程数（默认 4） | `8` |
| `fixFileDir` | 修复 SQL 文件目录（默认 `./fixsql`） | `/data/fixsql` |
| `logbin` | sql_log_bin 开关（ON/OFF，默认 ON） | `OFF` |
| `splitInsertOnDupKey` | multi-values INSERT 重复键自动拆分重试开关（ON/OFF，默认 ON；OFF 时整条语句失败） | `OFF` |
| `resultFile` | 自定义 CSV 报告输出路径 | `/tmp/repair-report.csv` |
| `resume` | 断点续传开关（OFF/ON/ASK，默认 OFF） | `ON` |
| `dstSslCa` | 目标端 CA 证书文件路径 | `/path/to/ca.pem` |
| `dstSslCert` | 目标端客户端证书文件路径 | `/path/to/client-cert.pem` |
| `dstSslKey` | 目标端客户端密钥文件路径 | `/path/to/client-key.pem` |
| `dstSslMode` | 目标端 SSL 模式（`DISABLED/PREFERRED/REQUIRED/VERIFY_CA/VERIFY_IDENTITY`，默认 `PREFERRED`） | `REQUIRED` |

### CSV 执行报告

执行完成后自动生成 CSV 报告，格式如下：
- **执行汇总**（位于报告最前）：总文件数、成功/失败数、INSERT/DELETE/ALTER/CREATE/DROP 各操作成功与失败数、总耗时
- **执行明细**：Schema、ObjectName、ObjectType（table/view/procedure/function/trigger）、INSERT/DELETE/ALTER/CREATE/DROP 各操作成功与失败数、耗时、执行失败原因

CSV 文件带有 UTF-8 BOM，可直接用 Excel/WPS 打开。报告写入失败仅输出 Warning，不影响修复流程。

## oracle_random_data_load

`oracle_random_data_load` 是 Oracle 随机数据写入工具，适用于压测、功能验证、迁移前预填充等场景。其核心能力包括：

1. 自动读取目标表元数据并按列类型生成随机值；
2. 主键列优先按“唯一值计划”生成，降低唯一键冲突概率；
3. 使用 `INSERT ALL ... SELECT 1 FROM DUAL` 批量写入，支持多 worker 并发；
4. 失败批次自动重试，并在必要时退化为逐行插入；
5. 提供实时进度日志和最终统计汇总。

### 快速使用

先编译：

```bash
go build -o oracle_random_data_load ./cmd/oracle_random_data_load
```

最小示例（写入 1000 行）：

```bash
./oracle_random_data_load \
  -dsn 'user="checksum" password="checksum" connectString="127.0.0.1:1521/gtchecksum" timezone="Asia/Shanghai" noTimezoneCheck="true"' \
  -table gtchecksum.t1 \
  -rows 1000
```

并发批量示例（4 并发、每批 500 行、输出日志文件）：

```bash
./oracle_random_data_load \
  -dsn 'user="checksum" password="checksum" connectString="127.0.0.1:1521/gtchecksum" timezone="Asia/Shanghai" noTimezoneCheck="true"' \
  -schema gtchecksum \
  -table t1 \
  -rows 200000 \
  -workers 4 \
  -batch-size 500 \
  -max-retries 2 \
  -progress-interval 2 \
  -log-file ./oracle_random_data_load.log
```

更多参数与完整案例见手册中的 [**oracle_random_data_load 工具使用说明**](./gt-checksum-manual.md#oracle_random_data_load-工具使用说明) 章节。

## 手册

[gt-checksum 手册](./gt-checksum-manual.md)

## 版本历史

[版本历史](./CHANGELOG.md)

## 配置参数

配置文件中所有参数的详解可参考模板文件 [gc-sample.conf](./gc-sample.conf)。

## 问题反馈

可以 [提交issue](https://gitee.com/GreatSQL/gt-checksum/issues) 查看或提交 gt-checksum 相关bug。

## 联系我们

扫码关注微信公众号

![输入图片说明](https://images.gitee.com/uploads/images/2021/0802/141935_2ea2c196_8779455.jpeg "greatsql社区-wx-qrcode-0.5m.jpg")
