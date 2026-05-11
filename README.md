[![](https://img.shields.io/badge/GreatSQL-官网-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-论坛-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-博客-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-Apache_v2.0-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/blob/master/LICENSE)
[![](https://img.shields.io/badge/release-3.0.0-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/releases)

# gt-checksum
**gt-checksum** 是GreatSQL社区开源的数据库校验及修复工具，支持 MySQL-family（MySQL/Percona/GreatSQL/MariaDB等）、Oracle 等主流数据库。

## 简介

MySQL DBA经常使用 **pt-table-checksum** 和 **pt-table-sync** 进行数据校验及修复，但这两个工具并不支持MySQL MGR架构，以及国内常见的上云下云业务场景，还有MySQL、Oracle间的异构数据库等多种场景。

因此，我们开发了 **gt-checksum** 工具，旨在支持更多业务场景并解决现有痛点。

## v3.0.0 关键变化

- **[功能新增]** 新增 `requirePK` 参数，支持在 struct 模式下为无主键表自动添加 `my_row_id` 不可见自增聚集索引列，主要用于 MySQL 单机实例迁移到 MGR 环境。
- **[功能新增]** repairDB 工具新增预执行报告功能，执行前自动展示修复 SQL 统计信息（文件数量、影响行数、预估 binlog 大小等）。
- **[功能新增]** repairDB 工具新增交互式确认机制和 `--dry-run` 参数，提升修复操作安全性。
- **[功能新增]** repairDB 工具新增 CSV 执行报告导出功能，自动生成包含执行汇总和明细的报告文件。
- **[功能新增]** repairDB 工具新增锁文件机制，防止重复执行修复操作，执行完成后自动生成 `.repairDB.lock` 文件。
- **[功能优化]** 增强分区表主键修复能力，为无主键分区表添加 `my_row_id` 时自动提取分区列并生成包含分区列的主键定义，确保符合 MySQL 分区表主键约束。
- **[功能优化]** 优化跨版本场景下表级 COLLATE 修复逻辑，当所有字段的 COLLATION 差异都是由表级 COLLATION 差异引起时，只需修改表级定义，避免逐列修改。
- **[问题修复]** 修复 SPATIAL KEY 索引 `SUB_PART` 被误用为 DDL 前缀长度的问题，避免生成部分索引触发 Error 1089。
- **[问题修复]** 修复 zerofill 属性被错误剥离导致字段定义差异检测不准确的问题。
- **[问题修复]** 修复跨版本 MySQL 场景下 `CREATE TABLE` 缺少 COLLATE 导致二次修复的问题，现在会显式添加源端 COLLATION 定义。
- **[问题修复]** 修复分区表 NULL 约束检查逻辑，对分区字段的 NULL 约束差异不再生成修复 SQL（分区字段通常有隐式的 NOT NULL 约束）。

更多详细变化详见 [CHANGELOG](./CHANGELOG.md)。

**gt-checksum** 支持以下几种常见业务需求场景：
1. **MySQL主从复制**：当主从复制中断较长时间后才发现，主从间数据差异太大。此时通常选择重建整个从库，如果利用 **pt-table-checksum**、**pt-table-sync** 先校验后修复，这个过程通常特别久，时间代价太大。而 **gt-checksum** 工作效率更高，可以更快校验出主从间数据差异并修复，这个过程时间代价小很多。
2. **MySQL MGR组复制**：MySQL MGR因故报错运行异常或某个节点异常退出时，在恢复时一般要先检查各节点间数据一致性，这时通常选择其中一个节点作为主节点，其余从节点直接复制数据重建，整个过程要特别久，时间代价大。在这种场景下选择使用 **gt-checksum** 效率更高。
3. **企业上下云**：在企业上云下云过程中要进行大量的数据迁移及校验工作，可能存在字符集原因导致个别数据出现乱码或其他情况，在迁移结束后进行完整的数据校验就很有必要了。
4. **异构迁移**：例如从Oracle迁移到MySQL等异构数据库迁移场景中，通常存在字符集不同、数据类型不同等多种复杂情况，也需要在迁移结束后进行完整的数据校验。
5. **定期数据校验**：在多节点高可用架构中，为了保证主节点出现异常后能安心切换，需要确保各节点间的数据一致性，通常要定期执行数据校验工作。
6. **MySQL版本升级时迁移数据**：在MySQL版本升级时（例如从5.6升级到8.0），需要将低版本中的数据迁移到高版本。
7. **MariaDB迁移到MySQL 8.0/8.4**：在 `MariaDB 10.x+ -> MySQL 8.0/8.4` 的迁移场景中，当前支持全部四种 `checkObject` 模式（`data`/`struct`/`routine`/`trigger`）的校验与修复。
8. **MariaDB实例间升级校验**：在 `MariaDB -> MariaDB` 的同序列或升级迁移场景中，当前支持 `data`、`struct`、`routine`、`trigger` 四种模式；支持升级方向，不支持 downgrade。

## 版本支持状态

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

## Roadmap

| 版本系列 | 目标版本 | 状态 | 方向 |
|---------|---------|------|------|
| v4.0.x | — | 规划中（需求收集中） | [欢迎提交需求issue](https://gitee.com/GreatSQL/gt-checksum/issues) |

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
gt-checksum version 1.3.0
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

每次校验结束后，当前目录下还会自动生成结果 CSV 文件（默认开启），例如：`gt-checksum-result-20260323195530.csv`。使用 Excel 或命令行可直接查看完整校验结果：

```bash
$ cat gt-checksum-result-20260323195530.csv

RunID,CheckTime,CheckObject,Schema,Table,ObjectName,ObjectType,IndexColumn,Rows,Diffs,Datafix,Mapping,Definer,Columns
20260323195530,2026-03-23 19:55:31,data,sbtest,sbtest2,sbtest2,table,id,4999,yes,file,,,
```

全列校验时最后一列 `Columns` 为空；当启用 `columns` 子集校验时，这一列会显示本次实际参与比对的列计划。

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

**注意**：由于是并行执行数据修复工作，修复过程中可能产生事务死锁冲突。`repairDB` 在检测到 MySQL deadlock（Error 1213）时，会自动对当前失败的事务块（`BEGIN ... COMMIT`）执行重试，最多重试 3 次；而不会重试整个 SQL 文件，从而降低主键重复冲突风险。建议修复结束后检查 `repairDB.log`：若死锁在 3 次重试内已恢复，可直接再次执行校验；若仍有未恢复死锁或其他错误，再手动处理对应 SQL 文件。

### repairDB CLI 参数

| 参数 | 说明 |
|---|---|
| `-conf` | 配置文件路径（默认 `gc.conf`） |
| `-f` / `--force` | 跳过交互式确认，直接执行修复 |
| `--dry-run` | 仅展示预执行统计报告，不执行实际修复 |
| `--result-file` | 自定义 CSV 报告输出路径（默认 `result/repairDB-result-<timestamp>.csv`） |

### repairDB 配置文件参数

| 参数 | 说明 | 示例 |
|---|---|---|
| `dstDSN` | 目标数据库连接串 | `mysql|user:pass@tcp(host:3306)/db` |
| `parallelThds` | 并发执行线程数（默认 4） | `8` |
| `fixFileDir` | 修复 SQL 文件目录（默认 `./fixsql`） | `/data/fixsql` |
| `logbin` | sql_log_bin 开关（ON/OFF，默认 ON） | `OFF` |
| `resultFile` | 自定义 CSV 报告输出路径 | `/tmp/repair-report.csv` |

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
go build -o oracle_random_data_load oracle_random_data_load.go
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

更多参数与完整案例见手册中的 [**oracle_random_data_load 工具使用说明**](./gt-checksum-manual.md) 章节。

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
