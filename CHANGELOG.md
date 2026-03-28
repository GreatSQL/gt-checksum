## 1.3.0
- [重大变更]: 移除 `fixFilePerTable` 参数，**每对象独立文件为唯一输出模式**；同步引入统一文件命名规则 `type.schema.object.sql`（`type` 为 `table`/`view`/`trigger`/`routine`，schema/object 名经 Percent 编码以安全处理特殊字符），各模式输出示例：`table.appdb.orders.sql`、`view.appdb.v_order.sql`、`trigger.appdb.trg_bi.sql`、`routine.appdb.proc_calc.sql`；旧版单文件（`datafix.sql`）路径及 `repairDB` 对应的单文件特判逻辑已同步移除，使用旧配置文件时该参数将被忽略并打印警告。
- [功能新增]: `checkObject=struct` 模式新增 VIEW（视图）支持（仅限 MySQL→MySQL）。自动识别 `tables` 参数中的视图对象并进行定义比对；差异时 `Diffs=yes`，`ObjectType=view`；修复 SQL 以 advisory 注释形式写入 fixsql 文件，不自动执行；`checkObject=data` 模式自动跳过视图对象，不再产生误报。VIEW 比对策略：① DEFINER 账号不计入差异；② `ALGORITHM=UNDEFINED`（默认值）与省略等价处理，不触发差异；③ SQL SECURITY 差异仅记录 Warn 日志，不计入 `Diffs=yes`（迁移时账号重构属常见合理变更）；④ 除定义文本外还对列元数据（类型、nullable、charset、collation）进行独立比对；定义文本一致但列元数据漂移时，advisory 中标注 `suggested SQL: none`；⑤ 跨 schema 映射（`db1.*:db2.*`）下，视图定义中的 schema 前缀参与归一化，不产生误报。终端 struct 模式结果表格新增 `ObjectType` 列，可直观区分 table / view 行。(issue #I899YZ)
- [功能新增]: 新增结果自动导出为 CSV 文件能力；新增参数 `resultExport`（`OFF` / `csv`，默认 `csv`）和 `resultFile`（自定义导出路径，默认 `gt-checksum-result-<RunID>.csv`）。CSV 文件为 UTF-8 BOM 编码，列头固定，可被 Excel 直接打开，包含所有校验结果（不受终端过滤影响）；`resultFile` 指定路径时如父目录不存在会自动创建。（issue #I6KMQF）
- [功能新增]: 新增参数 `terminalResultMode`（`all` / `abnormal`，默认 `all`）；设置为 `abnormal` 时终端仅显示存在差异的行（`yes` / `DDL-yes` / `warn-only`），CSV 始终输出完整结果；以上三个参数均支持 CLI 覆盖（`--resultExport` / `--resultFile` / `--terminalResultMode`）。
- [功能优化]: repairDB 执行调度从两阶段（DELETE→OTHER）升级为六阶段对象类型调度（DELETE→TABLE→VIEW→ROUTINE→TRIGGER→UNKNOWN）；基于文件名前缀（`table.`/`view.`/`routine.`/`trigger.`）及 `-DELETE-` 模式自动识别阶段；TABLE 阶段保留 shuffle 打散锁热点，其余阶段稳定排序；UNKNOWN 文件最后执行并打印 Warn；阶段间保持硬屏障，前序阶段失败则后续不再启动。
- [功能优化]: repairDB `main()` 重构为 `run() error` 模式，确保 defer 资源（`logFile`）在所有退出路径均能正确释放；引入 `io.MultiWriter` 将日志同时写入文件和标准输出，消除原有 `log.Printf`+`fmt.Printf` 双写冗余。
- [功能优化]: `ObjectTypeMap` 元数据查询性能优化；引入候选 schema 约束机制（`CandidateSchemas`），将 `INFORMATION_SCHEMA.TABLES` 扫描范围从实例全量收窄为本轮实际涉及的 schema 列表（`WHERE TABLE_SCHEMA IN (...)`），减少大实例上的不必要元数据开销；无候选集时保持原有全量扫描作为兜底，行为向后兼容。
- [测试完善]: 新增 `repairDB_test.go` 单元测试（共 15 个），覆盖 `detectObjectStage`、`classifySQLFiles`、`buildExecutionStages`、`prepareStageFiles` 四个核心调度函数，包含文件分类、阶段顺序、shuffle 行为、空阶段省略等场景；`scripts/regression-test.sh` 同步纳入 `repairDB` 单测执行步骤，确保日常回归可自动运行。
- [测试完善]: 补充 VIEW advisory SQL 修复相关单元测试，覆盖：DROP VIEW 不再出现的反向断言、`SET character_set_client = DEFAULT` 对称恢复断言、MariaDB uca1400 collation 自动映射路径验证。
- [测试完善]: 新增 VIEW struct 专项单元测试，覆盖：归一化规则（DEFINER/ALGORITHM/SQL SECURITY 剥离、空白折叠、body 大小写保留）、跨 schema 映射归一化、advisory SQL 生成（ALGORITHM 保留/SQL SECURITY 保留/WITH CHECK OPTION 保留/DEFINER 剥除）、fail-closed 路径（不可解析 DDL 输出 `suggested SQL: none`）、VIEW 缺失/多余/差异/列元数据漂移分支、data 模式过滤、ignoreTables 过滤、ObjectKind 路由；新增 `extractCandidateSchemas` 函数专项测试（正常去重、空 map 返回空切片）。
- [问题修复]: 修复 repairDB 跨阶段 session 变量泄漏问题：改为每阶段独立打开并关闭连接池，防止 `FOREIGN_KEY_CHECKS`、`UNIQUE_CHECKS` 等 session 级变量通过连接复用在阶段间扩散；此前共享连接池会导致 TABLE 阶段设置的会话变量在 VIEW/ROUTINE/TRIGGER/UNKNOWN 阶段中被意外继承。
- [问题修复]: 修复 VIEW advisory SQL 四项问题：① `SET character_set_client` 设置后缺少 `DEFAULT` 恢复，导致 repairDB 单连接执行时后续对象字符集上下文被污染；② advisory 块误含 `DROP VIEW IF EXISTS`，`CREATE OR REPLACE VIEW` 已可原子替换视图，先 DROP 引入缺失窗口及失败后永久删除风险；③ MariaDB 11.5+ 源端 uca1400 排序规则未映射为 MySQL 等价值，`SET collation_connection` 在 MySQL 8.0/8.4 执行报错；④ VIEW 列元数据硬差异路径误生成可执行重建 SQL，此类漂移源于底层基表变更，统一回退为 `suggested SQL: none`。
- [问题修复]: 此前已修复 连接oracle执行exec dbms_stats.gather_table_stats报错问题，本次补充测例（#I6NPC1）。

## 1.2.5
- [功能新增]: 新增 `MySQL 5.6/5.7/8.0/8.4` 同版本及升级链路支持，覆盖 `data`、`struct`、`routine`、`trigger` 四种校验模式；downgrade 或不支持的版本组合会在启动阶段直接拒绝执行。
- [功能新增]: 扩展 `MariaDB 10.x+ / 12.3+ -> MySQL 8.0/8.4` 支持至全部四种 `checkObject` 模式；新增参数 `mariaDBJSONTargetType` 支持 `MariaDB JSON` alias 改写为 `JSON`、`LONGTEXT` 或 `TEXT`；支持 `uca1400 -> uca0900` collation 自动映射，减少跨版本误报。
- [功能优化]: 统一结构语义比较与风险分级，将 `CHECK`、显示宽度、`utf8/utf8mb3`、`ZEROFILL`、`ROW_FORMAT`、默认 collation 漂移等差异收敛为 `warn-only` / `advisory-only` 分层输出；补齐 `routine` / `trigger` charset 元数据三维度比对；`checkObject=data` 新增 DSN charset 一致性预检，DDL 差异表稳定保留并显示 `DDL-yes`。
- [问题修复]: 修复多类结构比较误报（`CHECK` 括号噪音、主键 canonical key 残余、映射场景目标表名错误、collation advisory 重复输出等），以及 `MySQL 5.6/5.7` 查询 `INFORMATION_SCHEMA.STATISTICS.IS_VISIBLE` 的低版本兼容问题和 `checkObject=data` DDL-yes 链路结果丢失问题。
- [问题修复]: 修复 DSN `charset` 参数提取不完整、`MariaDB` 源端全局权限预检查误判，以及 `struct` / `routine` / `trigger` 模式连接池过大导致的 `Too many connections` 问题（#IEYE7P）。
- [测试完善]: MySQL 修复 SQL 生成路径新增 20 个单元测试，覆盖标识符引用（普通/含空格/含反引号/保留字）、ADD/DROP 索引、schema/table 名转义、外键名转义、routine/trigger DROP 转义、`normalizeAlterOperationContent` 正则提取等场景；Oracle 修复 SQL 生成路径新增 12 个单元测试，覆盖 `oracleIdentifier` 语义（简单大写裸名、小写→大写、含空格加双引号、已引用保留、内部双引号转义）、DROP/ADD 索引 Oracle 语法正确性、`FixAlterIndexSqlGenerate` 原样透传等场景。
- [问题修复]: 修复 MySQL 修复 SQL 中索引名、schema/table 名、外键名、routine 名、trigger 名未使用反引号转义的问题；当对象名包含空格、连字符、反引号或 MySQL 保留字时，生成的 `ALTER TABLE` 语句会导致执行失败；新增 `mysqlQuoteIdent()` 函数统一处理所有标识符引用，并以正则 `alterTablePrefixRe` 替换脆弱的 `strings.SplitN` 提取方式，消除 schema/table 名含空格时的解析错误。
- [问题修复]: 修复 Oracle 修复 SQL 使用错误 DDL 语法的问题；原实现对索引 DROP/ADD 均沿用 MySQL `ALTER TABLE` 语法，在 Oracle 上无法执行；改为：DROP 索引使用独立 `DROP INDEX schema.name;` 语句，ADD 索引使用 `CREATE [UNIQUE] INDEX schema.name ON schema.table (cols);` 语句；同时修复 `oracleIdentifier()` 语义：简单 ASCII 标识符返回大写裸名（无引号），避免加双引号后 Oracle 以大小写敏感方式查找对象导致 ORA-00942。

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
