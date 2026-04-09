[![](https://img.shields.io/badge/GreatSQL-官网-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-论坛-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-博客-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-Apache_v2.0-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/blob/master/LICENSE)
[![](https://img.shields.io/badge/release-1.3.0-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/releases)

# gt-checksum
**gt-checksum** 是GreatSQL社区开源的数据库校验及修复工具，支持 MySQL-family（MySQL/Percona/GreatSQL/MariaDB等）、Oracle 等主流数据库。

## 简介

MySQL DBA经常使用 **pt-table-checksum** 和 **pt-table-sync** 进行数据校验及修复，但这两个工具并不支持MySQL MGR架构，以及国内常见的上云下云业务场景，还有MySQL、Oracle间的异构数据库等多种场景。

因此，我们开发了 **gt-checksum** 工具，旨在支持更多业务场景并解决现有痛点。

## v1.4.0 关键变化

- **[功能新增]** 支持前缀索引/部分索引的检测与修复。

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

| 版本系列 | 最新版本 | 类型 | 发布时间 | EOL 时间 | 状态 | 说明 |
|---------|---------|------|---------|---------|------|------|
| v1.2.x | v1.2.5 | EOL | 2023-03-06 | 2026-12-31 | End of support | 已终止支持，不再提供任何更新（含 Bug Fix / 安全更新）；如遇问题请升级至 v1.3.x |
| v1.3.x | v1.3.0 | LTS | 2026-04-08 | 2028-04-08 | **活跃**（Bug Fix + Security） | 当前推荐生产版本；LTS 阶段仅接受缺陷修复与安全更新 |
| v1.4.x | v1.4.0 | 开发中 | MySQL 前缀索引差异检测与修复（已完成）；Oracle→MySQL 表结构校验与修复、函数索引、虚拟列、直方图等 |

> 说明：
>
> - 推荐生产环境使用 **v1.3.x**（当前活跃 LTS）；v1.2.x 将于 2026-12-31 终止支持，建议在此日期前完成升级。
> - v1.2.x 已进入 End of support：
>     - 不再提供任何形式的版本更新；
>     - 遇到任何新问题（兼容性、功能缺陷、稳定性或安全相关），请直接升级到 v1.3.x。
> - 支持策略：
>     - LTS（Long-Term Support）版本：自该大版本首个正式版发布之日起，技术支持周期为 2 年；进入 LTS 后仅接受 Bug Fix 与安全更新，不再引入新功能。
>     - 非 LTS 版本：自该大版本首个正式版发布之日起，技术支持周期为 1 年。

## Roadmap

| 版本系列 | 目标版本 | 状态 | 方向 |
|---------|---------|------|------|
| v1.5.x | — | 规划中（需求收集中） | 支持更多Oracle→MySQL数据类型，支持MySQL JSON索引、多值索引、全文索引、空间索引等|

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
