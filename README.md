[![](https://img.shields.io/badge/GreatSQL-官网-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-论坛-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-博客-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-Apache_v2.0-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/blob/master/LICENSE)
[![](https://img.shields.io/badge/release-1.3.0-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/releases)

# gt-checksum
**gt-checksum** 是GreatSQL社区开源的数据库校验及修复工具，支持MySQL、Oracle等主流数据库。

## 简介

MySQL DBA经常使用 **pt-table-checksum** 和 **pt-table-sync** 进行数据校验及修复，但这两个工具并不支持MySQL MGR架构，以及国内常见的上云下云业务场景，还有MySQL、Oracle间的异构数据库等多种场景。

因此，我们开发了 **gt-checksum** 工具，旨在支持更多业务场景并解决现有痛点。

## v1.3.0 关键变化

- **[重大变更]** 移除 `fixFilePerTable` 参数，**每对象独立文件为唯一输出模式**；同步引入统一文件命名规则 `type.schema.object.sql`（示例：`table.appdb.orders.sql`、`view.appdb.v_order.sql`、`trigger.appdb.trg_bi.sql`、`routine.appdb.proc_calc.sql`）；schema/object 名中的特殊字符自动进行 Percent 编码。旧版单文件（`datafix.sql`）路径已移除；使用旧配置文件时该参数将被忽略并打印警告。
- **[功能新增]** `checkObject=struct` 模式新增 VIEW（视图）支持（仅限 MySQL→MySQL）：自动识别 `tables` 参数中的视图对象，对 VIEW 定义与列元数据进行两层比对；差异时 `Diffs=yes`，`ObjectType=view`；修复建议以 advisory 注释形式写入 fixsql 文件，不自动执行，DBA 手工审阅后决定是否应用；`checkObject=data` 模式自动跳过视图，不产生误报。VIEW 比对关键策略：DEFINER 不计入差异；`ALGORITHM=UNDEFINED` 与省略等价；SQL SECURITY 差异仅记 Warn 日志不计入 Diffs；支持跨 schema 映射（`db1.*:db2.*`）不误报；终端结果表格新增 `ObjectType` 列。
- **[功能新增]** 新增结果 CSV 自动导出能力：每次校验结束后可自动生成 `gt-checksum-result-<RunID>.csv`；文件为 UTF-8 BOM 编码（Excel 可直接打开），列头固定，包含所有校验对象的完整结果，不受终端过滤影响。通过新参数控制：`resultExport`（`OFF` / `csv`，默认 `csv`）和 `resultFile`（自定义导出路径；父目录不存在时自动创建）。（#I6KMQF）
- **[功能新增]** 新增 `terminalResultMode` 参数（`all` / `abnormal`，默认 `all`）：设为 `abnormal` 时终端仅显示存在差异的行，方便快速定位问题；CSV 始终导出完整结果；以上参数均支持 CLI 覆盖（`--resultExport` / `--resultFile` / `--terminalResultMode`）。
- **[功能优化]** `ObjectTypeMap` 元数据查询性能优化：将 `INFORMATION_SCHEMA.TABLES` 扫描范围从实例全量收窄为本轮实际涉及的 schema 列表，减少大实例上的不必要元数据开销。
- **[功能优化]** repairDB 执行调度升级为六阶段对象类型模型（DELETE→TABLE→VIEW→ROUTINE→TRIGGER→UNKNOWN），基于文件名前缀自动识别对象类型；TABLE 阶段保留 shuffle 打散锁热点，其余阶段稳定排序；阶段间硬屏障，前序阶段失败后续不启动；每阶段独立连接池，防止 session 变量跨阶段泄漏。
- **[问题修复]** 修复 `tables` / `ignoreTables` 参数使用部分通配符 `*`（如 `sbtest.t*`）时静默产生错误结果的问题：现在在参数校验阶段快速失败并打印明确提示，建议改用 `%`（如 `sbtest.t%`）；修复覆盖映射目标侧及 `ignoreTables`。
- **[问题修复]** 修复表不存在时输出的 `CheckObject` 列被硬编码为 `struct` 的问题（`checkObject=data` 时亦受影响），现在正确反映用户配置的校验模式；修复 `checkObject=struct` 模式下双端表均不存在时输出重复行的问题。
- **[问题修复]** 修复 `checkObject=data` 模式下连接池大小不足导致校验 hang 住的问题：将单侧连接池下限从 `parallelThds + 2` 调整为 `parallelThds*2 + 4`（最低 8），覆盖 `data` 模式两阶段并发 pipeline 的峰值连接需求；同时修复连接池 `Get()` 持锁阻塞死锁及关闭竞态问题。
- **[问题修复]** 修复 VIEW advisory SQL 的四项问题：① `SET character_set_client` 写入后缺少 `DEFAULT` 恢复导致会话变量泄漏——同一 fixsql 文件中后续对象的 DDL 会在被污染的字符集环境中执行；② advisory 块误含 `DROP VIEW IF EXISTS`——`CREATE OR REPLACE VIEW` 已可原子替换视图，先 DROP 引入缺失窗口且 CREATE 失败时视图被永久删除；③ MariaDB 11.5+ 源端（uca1400 排序规则）生成的 `SET collation_connection` 值不兼容 MySQL 8.0/8.4；④ VIEW 列元数据硬差异路径误生成可执行 `CREATE OR REPLACE VIEW`——此类漂移源于底层基表变更，统一回退为 `suggested SQL: none`。

更多详细变化详见 [CHANGELOG](./CHANGELOG.md)。

**gt-checksum** 支持以下几种常见业务需求场景：
1. **MySQL主从复制**：当主从复制中断较长时间后才发现，主从间数据差异太大。此时通常选择重建整个从库，如果利用 **pt-table-checksum**、**pt-table-sync** 先校验后修复，这个过程通常特别久，时间代价太大。而 **gt-checksum** 工作效率更高，可以更快校验出主从间数据差异并修复，这个过程时间代价小很多。
2. **MySQL MGR组复制**：MySQL MGR因故报错运行异常或某个节点异常退出时，在恢复时一般要先检查各节点间数据一致性，这时通常选择其中一个节点作为主节点，其余从节点直接复制数据重建，整个过程要特别久，时间代价大。在这种场景下选择使用 **gt-checksum** 效率更高。
3. **企业上下云**：在企业上云下云过程中要进行大量的数据迁移及校验工作，可能存在字符集原因导致个别数据出现乱码或其他情况，在迁移结束后进行完整的数据校验就很有必要了。
4. **异构迁移**：例如从Oracle迁移到MySQL等异构数据库迁移场景中，通常存在字符集不同、数据类型不同等多种复杂情况，也需要在迁移结束后进行完整的数据校验。
5. **定期数据校验**：在多节点高可用架构中，为了保证主节点出现异常后能安心切换，需要确保各节点间的数据一致性，通常要定期执行数据校验工作。
6. **MySQL版本升级时迁移数据**：在MySQL版本升级时（例如从5.6升级到8.0），需要将低版本中的数据迁移到高版本。
7. **MariaDB迁移到MySQL 8.0/8.4**：在 `MariaDB 10.x+ -> MySQL 8.0/8.4` 的迁移场景中，当前支持全部四种 `checkObject` 模式（`data`/`struct`/`routine`/`trigger`）的校验与修复。

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

RunID,CheckTime,CheckObject,Schema,Table,ObjectName,ObjectType,IndexColumn,Rows,Diffs,Datafix,Mapping,Definer
20260323195530,2026-03-23 19:55:31,data,sbtest,sbtest2,sbtest2,table,id,4999,yes,file,,
```

如需只在终端显示差异行，可配置 `terminalResultMode=abnormal`（CSV 仍输出完整结果）：

```bash
$ gt-checksum -c ./gc.conf --terminalResultMode abnormal
```

查看运行目录下是否生成修复SQL文件目录，例如：fixsql-20260129154514

执行 repairDB 工具进行数据修复并查看执行结果：

```bash
$ ./repairDB ./fixsql-20260129154514 && cat ./repairDB.log

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

v1.3.0 新增参数如下：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `resultExport` | `csv` | 结果导出格式，`OFF` 不生成文件，`csv` 自动生成 CSV |
| `resultFile` | 空 | 自定义 CSV 输出路径，空时自动命名为 `gt-checksum-result-<RunID>.csv` |
| `terminalResultMode` | `all` | 终端输出模式，`all` 显示全部，`abnormal` 仅显示差异行 |

以上三个参数均支持 CLI 覆盖：`--resultExport`、`--resultFile`、`--terminalResultMode`。

## 问题反馈

可以 [提交issue](https://gitee.com/GreatSQL/gt-checksum/issues) 查看或提交 gt-checksum 相关bug。

## 联系我们

扫码关注微信公众号

![输入图片说明](https://images.gitee.com/uploads/images/2021/0802/141935_2ea2c196_8779455.jpeg "greatsql社区-wx-qrcode-0.5m.jpg")
