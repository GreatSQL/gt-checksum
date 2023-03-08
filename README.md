[![](https://img.shields.io/badge/GreatSQL-官网-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-论坛-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-博客-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-Apache_v2.0-blue.svg)](https://gitee.com/GreatSQL/GreatSQL/blob/master/LICENSE)
[![](https://img.shields.io/badge/release-1.2.0-blue.svg)](https://gitee.com/GreatSQL/gt-checksum/releases/tag/1.2.0)

# 关于 gt-checksum
gt-checksum是GreatSQL社区开源的一款静态数据库校验修复工具，支持MySQL、Oracle等主流数据库。

# 特性
---
MySQL DBA最常用的数据校验&修复工具应该是Percona Toolkit中的pt-table-checksum和pt-table-sync这两个工具，不过这两个工具并不支持MySQL MGR架构，以及国内常见的上云下云业务场景，还有MySQL、Oracle间的异构数据库等多种场景。

GreatSQL开源的gt-checksum工具可以满足上述多种业务需求场景，解决这些痛点。

gt-checksum工具支持以下几种常见业务需求场景：
1. **MySQL主从复制**：主从复制中断后较长时间才发现，且主从间差异的数据量太多，这时候通常基本上只能重建复制从库，如果利用pt-table-checksum先校验主从数据一致性后，再利用pt-table-sync工具修复差异数据，这个过程要特别久，时间代价太大。
2. **MySQL MGR组复制**：MySQL MGR因故崩溃整个集群报错退出，或某个节点异常退出，在恢复MGR集群时一般要面临着先检查各节点间数据一致性的需求，这时通常为了省事会选择其中一个节点作为主节点，其余从节点直接复制数据重建，这个过程要特别久，时间代价大。
3. **上云下云业务场景**：目前上云下云的业务需求很多，在这个过程中要进行大量的数据迁移及校验工作，如果出现字符集改变导致特殊数据出现乱码或其他的情况，如果数据迁移工具在迁移过程中出现bug或者数据异常而又迁移成功，此时都需要在迁移结束后进行一次数据校验才放心。
4. **异构迁移场景**：有时我们会遇到异构数据迁移场景，例如从Oracle迁移到MySQL，通常存在字符集不同，以及数据类型不同等情况，也需要在迁移结束后进行一次数据校验才放心。
5. **定期校验场景**：作为DBA在维护高可用架构中为了保证主节点出现异常后能够快速放心切换，就需要保证各节点间的数据一致性，需要定期执行数据校验工作。

以上这些场景，都可以利用gt-chcksum工具来满足。

# 下载
---
可以 [这里](https://gitee.com/GreatSQL/gt-checksum/releases) 下载预编译好的二进制文件包，已经在Ubuntu、CentOS、RHEL等多个下测试通过。

如果需要校验Oracle数据库，则还需要先下载Oracle数据库相应版本的驱动程序，并配置驱动程序使之生效。例如：待校验的数据库为Oracle 11-2，则要下载Oracle 11-2的驱动程序，并使之生效，否则连接Oracle会报错。详细方法请见下方内容：[**下载配置Oracle驱动程序**](#%E4%B8%8B%E8%BD%BD%E9%85%8D%E7%BD%AEoracle%E9%A9%B1%E5%8A%A8%E7%A8%8B%E5%BA%8F) 。

# 快速运行
---
```shell
# 不带任何参数
shell> ./gt-checksum
If no parameters are loaded, view the command with --help or -h

# 查看版本号
shell> ./gt-checksum -v
gt-checksum version 1.2.0

# 查看使用帮助
shell> ./gt-checksum -h
NAME:
   gt-checksum - A opensource table and data checksum tool by GreatSQL

USAGE:
   gt-checksum [global options] command [command options] [arguments...]
...

# 数据库授权
# 想要运行gt-checksum工具，需要至少授予以下几个权限
# MySQL端
# 1.全局权限
#  a.`REPLICATION CLIENT`
#  b.`SESSION_VARIABLES_ADMIN`，如果是MySQL 8.0版本的话，MySQL 5.7版本不做这个要求
# 2.校验数据对象
#  a.如果`datafix=file`，则只需要`SELECT`权限
#  b.如果`datafix=table`，则需要`SELECT、INSERT、DELETE`权限
#
# 假设现在要对db1.t1做校验和修复，则可授权如下

mysql> GRANT REPLICATION CLIENT, SESSION_VARIABLES_ADMIN ON *.* to ...;
mysql> GRANT SELECT, INSERT, DELETE ON db1.t1 to ...;

# Oracle端
# 1.全局权限
#  a.`SELECT ANY DICTIONARY`
# 2.校验数据对象
#  a.如果`datafix=file`，则只需要`SELECT ANY TABLE`权限
#  b.如果`datafix=table`，则需要`SELECT ANY TABLE、INSERT ANY TABLE、DELETE ANY TABLE`权限


# 指定配置文件，开始执行数据校验，示例：
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


# 使用命令行传参方式执行数据校验
shell> ./gt-checksum -S type=mysql,user=checksum,passwd=Checksum@123,host=172.16.0.1,port=3306,charset=utf8 -D type=mysql,user=checksum,passwd=Checksum@123,host=172.16.0.2,port=3306,charset=utf8 -t test.t2 -nit yes
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

# 下载配置Oracle驱动程序
---
如果需要校验Oracle数据库，则还需要先下载Oracle数据库相应版本的驱动程序。例如：待校验的数据库为Oracle 11-2，则要下载Oracle 11-2的驱动程序，并使之生效，否则连接Oracle会报错。

## 下载Oracle Instant Client
从 [https://www.oracle.com/database/technologies/instant-client/downloads.html](https://www.oracle.com/database/technologies/instant-client/downloads.html) 下载免费的Basic或Basic Light软件包。

- oracle basic client, instantclient-basic-linux.x64-11.2.0.4.0.zip

- oracle sqlplus, instantclient-sqlplus-linux.x64-11.2.0.4.0.zip

- oracle sdk, instantclient-sdk-linux.x64-11.2.0.4.0.zip

## 配置oracle client并生效
```shell
shell> unzip instantclient-basic-linux.x64-11.2.0.4.0.zip
shell> unzip instantclient-sqlplus-linux.x64-11.2.0.4.0.zip
shell> unzip instantclient-sdk-linux.x64-11.2.0.4.0.zip
shell> mv instantclient_11_2 /usr/local
shell> echo "export LD_LIBRARY_PATH=/usr/local/instantclient_11_2:$LD_LIBRARY_PATH" >> /etc/profile
shell> source /etc/profile
```

# 源码编译
gt-checksum工具采用GO语言开发，您可以自行编译生成二进制文件。

编译环境要求使用golang 1.17及以上版本。

请参考下面方法下载源码并进行编译：
```shell
shell> git clone https://gitee.com/GreatSQL/gt-checksum.git
shell> go build -o gt-checksum gt-checksum.go
shell> chmod +x gt-checksum
shell> mv gt-checksum /usr/local/bin
```

也可以直接利用Docker环境编译，在已经准备好Docker运行环境的基础上，执行如下操作即可：
```shell
shell> git clone https://gitee.com/GreatSQL/gt-checksum.git
shell> cd gt-checksum
shell> DOCKER_BUILDKIT=1 docker build --build-arg VERSION=v1.2.0 -f Dockerfile -o ./ .
shell> cd gt-checksum-v1.2.0
shell> ./gt-checksum -v
gt-checksum version 1.2.0
```
这就编译完成并可以开始愉快地玩耍了。

# 使用文档
---
- [gt-checksum manual](https://gitee.com/GreatSQL/gt-checksum/blob/master/docs/gt-checksum-manual.md)


# 版本历史
---
- [版本历史](https://gitee.com/GreatSQL/gt-checksum/blob/master/relnotes/CHANGELOG.zh-CN.md)


# 已知缺陷
---
截止最新的1.2.0版本中，当表中有多行数据是完全重复的话，可能会导致校验结果不准确，详见 [已知缺陷](https://gitee.com/GreatSQL/gt-checksum/blob/master/docs/gt-checksum-manual.md#已知缺陷) 。

# 问题反馈
---
- [问题反馈 gitee](https://gitee.com/GreatSQL/gt-checksum/issues)


# 联系我们
---

扫码关注微信公众号

![输入图片说明](https://images.gitee.com/uploads/images/2021/0802/141935_2ea2c196_8779455.jpeg "greatsql社区-wx-qrcode-0.5m.jpg")
