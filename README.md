[![](https://img.shields.io/badge/GreatSQL-官网-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-论坛-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-博客-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-Apache_v2.0-blue.svg)](https://gitee.com/yejr/gt-checksum/blob/master/LICENSE)
[![](https://img.shields.io/badge/release-1.2.1-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/releases/tag/1.2.1)

# gt-checksum
**`**gt-checksum** 是GreatSQL社区开源的数据库校验及修复工具，支持MySQL、Oracle等主流数据库。

## 简介

MySQL DBA最常用的数据校验及修复工具通常是 **pt-table-checksum** 和 **pt-table-sync**，但这两个工具并不支持MySQL MGR架构，以及国内常见的上云下云业务场景，还有MySQL、Oracle间的异构数据库等多种场景。

因此，我们开发了 **gt-checksum** 工具，旨在解决MySQL目标是支持更多业务需求场景，解决一些痛点。

**gt-checksum** 支持以下几种常见业务需求场景：
1. **MySQL主从复制**：当主从复制中断较长时间后才发现，主从间数据差异太大。此时通常选择重建整个从库，如果利用**pt-table-checksum**、**pt-table-sync** 先校验后修复，这个过程通常特别久，时间代价太大。而 **gt-checksum** 工作效率更高，可以更快校验出主从间数据差异并修复，这个过程时间代价小很多。
2. **MySQL MGR组复制**：MySQL MGR因故报错运行异常或某个节点异常退出时，在恢复时一般要先检查各节点间数据一致性，这时通常选择其中一个节点作为主节点，其余从节点直接复制数据重建，整个过程要特别久，时间代价大。在这种场景下选择使用 **gt-checksum** 效率更高。
3. **企业上下云**：在企业上云下云过程中要进行大量的数据迁移及校验工作，可能存在字符集原因导致个别数据出现乱码或其他情况，在迁移结束后进行完整的数据校验就很有必要了。
4. **异构迁移**：例如从Oracle迁移到MySQL等异构数据库迁移场景中，通常存在字符集不同、数据类型不同等多种复杂情况，也需要在迁移结束后进行完整的数据校验。
5. **定期数据校验**：在多节点高可用架构中，为了保证主节点出现异常后能安心切换，需要确保各节点间的数据一致性，通常要定期执行数据校验工作。

## 下载

可以 [这里](https://gitee.com/GreatSQL/gt-checksum/releases) 下载预编译好的二进制文件包，已经在Ubuntu、CentOS、RHEL等多个下测试通过。

如果需要校验Oracle数据库，则还需要先下载Oracle数据库相应版本的驱动程序，并配置驱动程序使之生效。例如：待校验的数据库为Oracle 11-2，则要下载Oracle 11-2的驱动程序，并使之生效，否则连接Oracle会报错。详细方法请见下方内容：[**下载配置Oracle驱动程序**](./docs/gt-checksum-manual.md#下载配置Oracle驱动程序)。

## 快速运行
- 不带任何参数

```bash
shell> ./gt-checksum
If no parameters are loaded, run the command with -h or --help
```

- 查看版本号

```bash
shell> ./gt-checksum -v
gt-checksum version 1.2.1
```

- 查看使用帮助

```bash
shell> ./gt-checksum -h
NAME:
   gt-checksum - opensource database checksum and sync tool by GreatSQL

USAGE:
   gt-checksum [global options] command [command options] [arguments...]
```

- 指定配置文件方式，执行数据校验

```bash
shell> ./gt-checksum -f ./gc.conf
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

- 使用命令行传参方式，执行数据校验

```bash
shell> ./gt-checksum -S driver=mysql,user=checksum,passwd=Checksum@123,\
host=172.16.0.1,port=3306,charset=utf8 \
-D driver=mysql,user=checksum,passwd=Checksum@123,\
host=172.16.0.2,port=3306,charset=utf8 -t test.t2 -nit yes
-- gt-checksum init configuration files --
-- gt-checksum init log files --
-- gt-checksum init check parameter --
-- gt-checksum init check table name --
-- gt-checksum init check table column --
-- gt-checksum init check table index column --
-- gt-checksum init source and dest transaction snapshoot conn pool --
-- gt-checksum init cehck table query plan and check data --
begin checkSum index table SCOTT.A5
[█                    ]100%  task:       1/1
table SCOTT.A5 checksum complete

** gt-checksum Overview of results **
Check time:  0.29s (Seconds)
Schema  Table   IndexCol        checkMod        Rows    Differences     Datafix
test    t2      id              rows            10,10   no              file
```

> 开始执行数据校验钱，要先在源和目标数据库创建相应的专属账号并授权。详情参考：[**gt-checksum 手册**](./docs/gt-checksum-manual.md#数据库授权)。

## 手册
---
- [gt-checksum 手册](./docs/gt-checksum-manual.md)

## 版本历史
---
- [版本历史](./relnotes/CHANGELOG.zh-CN.md)

## 已知缺陷
---
截止最新的1.2.1版本中，当数据表没有显式主键，且表中有多行数据是重复的，可能会导致校验结果不准确，详见 [已知缺陷](./docs/gt-checksum-manual.md#已知缺陷) 。

## 问题反馈
---
- [问题反馈 gitee](https://gitee.com/GreatSQL/gt-checksum/issues)


## 联系我们
---

扫码关注微信公众号

![输入图片说明](https://images.gitee.com/uploads/images/2021/0802/141935_2ea2c196_8779455.jpeg "greatsql社区-wx-qrcode-0.5m.jpg")
