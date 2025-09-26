[![](https://img.shields.io/badge/GreatSQL-官网-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-论坛-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-博客-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-Apache_v2.0-blue.svg)](https://gitee.com/yejr/gt-checksum/blob/master/LICENSE)
[![](https://img.shields.io/badge/release-1.2.1-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/releases/tag/1.2.1)

# gt-checksum
**gt-checksum** 是GreatSQL社区开源的数据库校验及修复工具，支持MySQL、Oracle等主流数据库。

## 简介

MySQL DBA经常使用 **pt-table-checksum** 和 **pt-table-sync** 进行数据校验及修复，但这两个工具并不支持MySQL MGR架构，以及国内常见的上云下云业务场景，还有MySQL、Oracle间的异构数据库等多种场景。

因此，我们开发了 **gt-checksum** 工具，旨在解决MySQL目标是支持更多业务需求场景，解决一些痛点。

**gt-checksum** 支持以下几种常见业务需求场景：
1. **MySQL主从复制**：当主从复制中断较长时间后才发现，主从间数据差异太大。此时通常选择重建整个从库，如果利用 **pt-table-checksum**、**pt-table-sync** 先校验后修复，这个过程通常特别久，时间代价太大。而 **gt-checksum** 工作效率更高，可以更快校验出主从间数据差异并修复，这个过程时间代价小很多。
2. **MySQL MGR组复制**：MySQL MGR因故报错运行异常或某个节点异常退出时，在恢复时一般要先检查各节点间数据一致性，这时通常选择其中一个节点作为主节点，其余从节点直接复制数据重建，整个过程要特别久，时间代价大。在这种场景下选择使用 **gt-checksum** 效率更高。
3. **企业上下云**：在企业上云下云过程中要进行大量的数据迁移及校验工作，可能存在字符集原因导致个别数据出现乱码或其他情况，在迁移结束后进行完整的数据校验就很有必要了。
4. **异构迁移**：例如从Oracle迁移到MySQL等异构数据库迁移场景中，通常存在字符集不同、数据类型不同等多种复杂情况，也需要在迁移结束后进行完整的数据校验。
5. **定期数据校验**：在多节点高可用架构中，为了保证主节点出现异常后能安心切换，需要确保各节点间的数据一致性，通常要定期执行数据校验工作。

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
gt-checksum version 1.2.3
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

> 开始执行数据校验前，要先在源和目标数据库创建相应的专属账号并授权。详情参考：[**gt-checksum 手册**](./gt-checksum-manual.md#数据库授权)。

## 手册

[gt-checksum 手册](./gt-checksum-manual.md)

## 版本历史

[版本历史](./CHANGELOG.zh-CN.md)

## 配置参数

配置文件中所有参数的详解可参考模板文件 [gc-sample.conf](./gc-sample.conf)。

## 问题反馈

可以 [提交issue](https://gitee.com/GreatSQL/gt-checksum/issues) 查看或提交 gt-checksum 相关bug。

## 联系我们

扫码关注微信公众号

![输入图片说明](https://images.gitee.com/uploads/images/2021/0802/141935_2ea2c196_8779455.jpeg "greatsql社区-wx-qrcode-0.5m.jpg")
