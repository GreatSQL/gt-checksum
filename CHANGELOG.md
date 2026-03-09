## 1.2.4
- 支持Oracle=>MySQL的单向数据校验和修复，目前支持NUMBER/CHAR/NCHAR/VARCHAR2/FLOAT/DECIMAL/DATE/TIMESTAMP/CLOB等多个常用类型
- 新增Oracle 随机数据加载工具oracle_random_data_load
- 配置文件中去掉"[DSNs],[Schema]"等一级标签
- 配置文件中存在重复配置参数时，只读取最后一条
- 将fixFileName更名为fixFileDir，用于自定义修复SQL文件存放目录；该目录名以"fixsql-时间戳"，会自动创建；并且当该目录不为空时会报错退出
- 新增参数fixFilePerTable，设置为ON时，针对每个表生成独立的SQL文件，并且按照fixTrxNum切分成多个子文件，便于并行修复；默认值为OFF，即所有修复SQL语句都合并到一个文件中
- 新增repairDB程序，读取gc.conf配置参数，读取fixFileDir目录下所有.sql文件，完成并行修复数据库；也可以自行指定修复SQL文件目录，例如 repairDB ./my-fixsql-dir。在某个测例中，共有1218个fixsql文件，24301776行SQL语句，所有文件共6.5GB，参数`parallelThds=4/chunkSize=20000/fixTrxNum=20000`，修复耗时9m43.936s，表现相当优异
- 新增sqlWhere参数，用于定义校验数据时的WHERE条件，对于大表中只有小部分数据不一致的场景校验效率有极大提升；默认值为空字符串""，表示不添加WHERE子句条件，校验所有数据行
- 支持没有任何索引（包括隐藏主键my_row_id）的表数据校验和修复
- 程序运行时，检查 logFile 文件是否为空，如果不为空则将其重命名为 logFile-时间戳，例如 gt-checksum.log 重命名为 gt-checksum.log-20230801100000
- 支持读取gc.conf配置文件时,忽略配置参数中的多余空格
- 新增DDL不一致检测功能：当源端与目标端DDL定义不一致时，校验报告的Diffs列显示为"DDL-yes"，准确区分DDL差异和数据差异
- 支持MySQL 8.0 GIPK（自动生成的不可见主键my_row_id）场景下的DDL差异识别
- 新增索引列目标端存在性验证，当索引列（如my_row_id）在目标端不存在时，提前标记DDL不一致并跳过无意义的数据比较
- 优化INSERT和DELETE语句合并方案，分别设置不同上限，避免DELETE语句影响太多行数据而产生太久锁等待，反倒影响修复效率
- 新增参数fixTrxSize（默认4MB）用于限制单个fixsql文件大小；新增insertSqlSize和deleteSqlSize（单位KB）用于分别控制INSERT和DELETE合并SQL大小
- 优化内存使用：当有大量数据存在差异时，运行耗时降低12%的前提下，最高内存使用降低96%，效果显著
- 新增参数showActualRows表示是否统计各表精确行数并优化相应的查询SQL，可设置为 [ON | OFF]，默认值：ON
- 优化fixsql文件写入方式，不再采用临时文件写入，改为流式直接写入目标文件
- 新增对MySQL中的struct, routine两种校验场景的支持，包括 DELIMITER、PROCEDURE/FUNCTION 定义包含COMMENT属性
- 新增支持 MySQL -> MySQL struct 场景下的表级 AUTO_INCREMENT 差异检测与修复
- Bugs fixed
  - 修复了ignoreTables参数无效问题
  - 修复了tables参数不支持%通配符问题，重新支持包括库名映射场景下的%通配符用法，例如"db1.t%:db2.t%"
  - 修复了当数据表没有显式主键，且表中有多行数据是重复的，可能会导致校验结果不准确问题
  - 修复元数据查询链路并发共享状态导致的 DDL-yes 随机漂移问题

## 1.2.3
- 增加内存使用量限制，且当内存使用量超过该值时，会自动调低parallelThds,queueSize,chunkSize这几个影响性能的参数，并进行GC操作
- 增加数据库名映射功能，支持将源端数据库名映射为目标端不同名数据库，例如"db1.*:db2.*"
- 删除checkMode和ratio参数，也即总是校验所有数据，不再支持仅采样和仅查总数两种校验方式
- 删除ScheckMod参数，也即总是严格校验表结构，不只是校验列名
- 删除ScheckOrder参数，也即总是按照源端数据表中列的正序进行校验
- 删除ScheckFixRule参数，即总是针对目标端执行修复数据方案，如果需要反向修复，自行调整DSN配置即可
- 参数checkObject的可选值进行调整合并，struct包含原来的struct|index|partitions|foreign等几个值
- 对同一个表既有删索引又有加索引等操作时，两个DDL合并一起，提升效率
- 支持在fix sql中同时设置索引的不可见属性
- 支持在只修改主键字段名大小写时，无需重建整个主键索引，避免数据丢失
- 支持在只修改字段名大小写时，将删除重建字段改为CHANGE COLUMNN，避免数据丢失
- 支持添加自增且不可见主键和外键约束字段的修复语法（遗留问题：除了添加新字段，会多一个额外的ADD PRIMARY KEY/ADD CONSTRAINT操作，需要手动删掉，未来版本再修复）
- 支持当partition不一致时，仅生成报告（Diffs=no）但不生成fixSQL（生成提示信息，没有具体SQL）
- 支持生成外键约束（FOREIGN KEY）修复SQL
- 在checkObject=data时，当发现待检查表结构不一致时，略过该表，并加上skipped提示，同时避免被hang住
- 支持合并fixSQL中的DELETE操作，提高SQL执行效率
- 支持生成fixSQL时，如果是INSERT操作，则总是声明所有字段名，应对隐藏字段情况
- 支持生成fixSQL时，在文件头加上字符集设定（从dstDSN参数中的"charset"值获取）、临时禁用外键和唯一约束检查
- 支持生成fixSQL时，在CREATE TABLE/TRIGGER/PROCEDURE/FUNCTION前面加上相应的schema name
- Bugs fixed
  - 修复生成fixSQL时无法正确使用索引问题
  - 修复生成fixSQL时无法正确处理映射规则问题
  - 修复生成fixSQL时指定 DEFAULT 'null' 导致语法错误问题
  - 修复了当目标端缺少某个表时，生成的fixSQL是ALTER TABLE ... ADD COLUMN而不是CREATE TABLE问题
  - 修复了当索引不一致时，Diffs结果仍显示为no的问题
  - 修复了fixSQL中存在`ADD COLUMN`时，字段名自动变大写的问题，保留源端字段名大小写
  - 修复了fix SQL中包含empty语法错误问题
  - 修复了checkObject=data时，当待检查表为空，疑似会进入死循环问题
  - 修复了特殊字符（如`\\, \', \", \n, \r`等字符）导致校验结果不正确问题：https://greatsql.cn/thread-908-1-1.html
  - 修复了当执行数据校验时发现表结构不一致导致校验过程可能被hang住的问题
  - 修复了将date_format函数误识别为字段名问题
  - 修复了testcase脚本错误
  - 修复了因为表结构CHARSET&COLLATE或COMMENT不一致导致执行时可能报告字段不存在的问题
  - 修复了checkObject=data时,如果指定的tables不存在时可能被hang住的问题
  - 修复了字符串列末尾多一个空格可能导致校验不准的问题
  - 修复了当不一致的数据量超过1万行时，可能校验不正确且生成fixSQL也不正确的问题

相对于 1.2.1 版本，数据校验性能及内存消耗表现测试结果如下：

| 耗时(秒) | v123   | v121   | 变化     |
|----------|--------|--------|----------|
| sbtest   | 94.03  | 106.19 | -11.45%  |
| tpch     | 418.72 | 350.33 | 19.52%   |
| bmsql    | 15.29  | 9.663  | 58.23%   |
| 总耗时   | 528.04 | 466.183| 13.27%   |

| 内存(rss, size) | v123 | v121 | 变化 |
|----------------|------|------|------|
| sbtest |1,873,360, 2,325,548 | 3,708,140, 4,340,072 | -49.48%, -46.42% |
| tpch | 234,892, 443,100 | 437,660, 643,032 | -46.33%, -31.09% |
| bmsql | 594,776, 793,140 | 750,824, 1,018,712 | -20.78%, -22.14% |

| 测试数据 | 整库容量 | 整库数据量 |
|--------|----------|------------|
| sbtest | 21G      | 18,000,006 |
| tpch   | 20G      | 86,586,082 |
| bmsql  | 327M     | 1,596,239  |

结论：总耗时有所增加，但内存消耗下降明显，对于数据量较大且内存较小的场景比较有利。

## 1.2.2
- 合并`jointIndexChanRowCount`和`singleIndexChanRowCount`两个参数为新的参数`chunkSize`
- 不再支持命令行传参方式调用，仅支持配置文件方式调用，命令行参数仅支持"-h", "-v", "-c"等几个必要的参数
- 删除极简模式，默认支持配置文件中只有srcDSN, dstDSN, tables等几个参数
- 参数名`lowerCaseTableNames`变更为`caseSensitiveObjectName`，更好理解
- 新增参数`memoryLimit`，用于限制内存使用量，防止OOM
- 优化校验结果输出，Rows的值改为精确值，此外不再频繁输出刷屏
- 参数`logFile`支持日期时间格式，例如：gt-checksum-%Y%m%d%H%M%S.log
- 优化校验结果进度条及汇总报告内容，增加各表、各阶段各自的耗时
- 修复无法使用普通索引和无索引时校验失败的问题
- Bugs fixed
  - 命令行 ScheckFixRule 参数传入失败问题 #IA84QZ
  - 检查不出来数据不一致问题 #I8HSQB
  - 空表直接报错以及表名大小问题 #I8SEPI
  - out of memory问题 #I89A2J
  - 校验输出结果中Rows数值不精确问题 #I830CY
  - 表统计信息为空导致运行失败问题 #I7Y64J

## 1.2.1
新增表结构校验、列类型校验等新特性及修复数个bug。
`gt-checksum` 修复bug及新增功能如下：
- 新增表结构的校验，并生成修复语句，支持对象包括如下(源目标端校验表都存在)：
- 支持列的数据类型的校验及修复
- 支持列的字符集及校验级的校验及修复（MySQL支持字符串校验，oracle不校验）
- 支持列是否允许null的校验及修复
- 支持列的默认值是否一致的校验及修复
- 支持列的乱序的验证及修复
- 支持列数据存在多列、少列的验证及修复
- 支持列的comment的校验及修复
- 支持宽松模式和严谨模式校验
- 支持校验列时是按正序校验还是乱序校验
- 支持修复语句列属性的指定依据，是按源端校验还是目标端校验
- 修复索引校验并生成修复语句时出现的空指针错误
- 修复因为8.0数据库查询条件没有产生where关键字导致的sql执行失败
- 优化代码（参数input输入部分），精简代码，并结构化处理
- 修复因数据库开启lowerCaseTableNames不生效导致无法校验区分大小写的表

## 1.2.0
gt-checksum正式发布，版本号1.2.0，可以满足绝大多数场景下的数据校验&修复需求，尤其是MySQL、Oracle间的异构数据库场景。

`gt-checksum` 工具主要功能特性有：
- 支持主从复制、MGR以及MySQL、Oracle间的数据校验&修复；
- 数据库名、表名设置支持多种正则表达式
- 支持多种字符集
- 支持设置表名大小写敏感
- 支持多种数据校验模式，数据、表结构、索引、分区、外键、存储过程等
- 支持多种数据校验方式，全量校验，抽样校验和行数校验
- 支持多种数据修复模式，校验完毕后直接修复或是生成修复SQL文件再自行手动处理
- 支持校验无索引表
- 支持并发多线程校验
- 更好支持大表数据校验，效率更高，且基本不会发生OOM等问题
