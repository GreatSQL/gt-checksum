## 1.3.0
- [功能新增]: 新增在 `checkObject=data` 模式下支持只校验部分字段功能，新增 `columns` 参数用于设置校验字段列表，该参数支持不同表名、字段名映射。支持同名列和源端→目标端列名映射；只比较选中的业务列，存在差异时生成 `UPDATE` 修复SQL；通过 `extraRowsSyncToSource` 参数控制是否生成 `DELETE`；当源端数据更多时生成 `columns-advisory.<schema>.<table>.sql` 文件提示人工介入处理。(issue #I6KGOJ #I6KGXF)
- [功能新增]: `checkObject=struct` 模式新增 VIEW（视图）支持（仅限 MySQL→MySQL）；自动识别 `tables` 中的视图对象并比对定义与列元数据，差异时输出 `ObjectType=view`，修复建议以 advisory 形式写入 fixsql 文件，`checkObject=data` 模式会自动跳过视图对象。(issue #I899YZ)
- [功能新增]: 新增结果自动导出为 CSV 文件能力；新增参数 `resultExport`（`OFF` / `csv`，默认 `csv`）和 `resultFile`（自定义导出路径，默认输出到 `result/` 目录），并新增 `terminalResultMode`（`all` / `abnormal`，默认 `all`）控制终端是否只显示异常结果；以上参数均支持 CLI 覆盖。（issue #I6KMQF）
- [功能新增]: repairDB 新增 `logbin`（`ON` / `OFF`，默认 `ON`），可控制修复时是否写入 binlog。
- [功能优化]: 表结构修复在识别到“兼容的列重命名”场景时，改用 `CHANGE COLUMN` 代替 `DROP COLUMN + ADD COLUMN`，尽量保留列数据并减少高风险 DDL。
- [功能优化]: 表级 `charset/collation` 修复语句统一显式带上 `COLLATE`，降低跨版本迁移时被目标端默认排序规则偷偷改写的概率。
- [功能优化]: 移除 `fixFilePerTable` 参数，统一为“每对象独立修复文件”输出模式；fixsql 文件命名规则统一为 `type.schema.object.sql`，`fixFileDir` 默认值同步调整为固定目录 `fixsql`。
- [功能优化]: repairDB 执行调度升级为六阶段模型（DELETE→TABLE→VIEW→ROUTINE→TRIGGER→UNKNOWN），按对象类型分阶段执行；TABLE 阶段保留 shuffle 打散锁热点，其余阶段稳定排序，阶段间保持硬屏障并使用独立连接池。
- [功能优化]: repairDB 主流程重构为 `run() error` 模式，统一 defer 资源释放，并通过 `io.MultiWriter` 同时输出到日志文件和标准输出。
- [功能优化]: 元数据查询新增候选 schema 收窄策略，只扫描本轮实际涉及的 schema，减少大实例上的 `INFORMATION_SCHEMA.TABLES` 扫描开销。
- [测试完善]: 补齐 columns 模式专项测试，覆盖参数解析、选列查询、advisory 文件输出与转义、真实链路下的 PK 精度保留、CSV/终端 Columns 列展示等关键路径。
- [测试完善]: 新增 `scripts/regression-test-columns.sh` 端到端集成回归脚本（配套 `testcase/MySQL-columns-source.sql` / `testcase/MySQL-columns-target.sql` fixture），覆盖 TC-01～TC-08 共 8 个核心场景：非选中列差异忽略（TC-01）、选中列差异修复收敛（TC-02）、source-only advisory 输出（TC-03）、简单列名语法（TC-04）、跨表列名映射修复（TC-05）、无主键 DDL-yes 预期行为（TC-06）、target-only 行 + `extraRowsSyncToSource` DELETE 修复（TC-07）、简单语法多字段（TC-08）；附 `TC-ORA-01` Oracle stub 错误处理测例（`--enable-oracle` 启用）；支持多轮修复收敛验证、`--dry-run` 预览模式、可配置超时（`--timeout`）与产物目录（`--artifacts-dir`）。
- [测试完善]: 新增 `repairDB_test.go` 单元测试，覆盖文件分类、阶段顺序、shuffle 行为、空阶段省略等核心调度逻辑；`scripts/regression-test.sh` 同步纳入 `repairDB` 单测执行步骤。
- [测试完善]: 补充 VIEW advisory SQL 与 VIEW struct 专项单测，覆盖 `DROP VIEW` 移除、`SET ... = DEFAULT` 对称恢复、MariaDB `uca1400` collation 映射、跨 schema 归一化、列元数据漂移等场景。
- [测试完善]: 新增 `EvaluateDataCheckPreflight` 回归测试，覆盖源端缺表、双端缺表、空检查列表、有效表、混合有效/异常、invisible 列不匹配等路径，防止 data 模式预检回归。
- [测试完善]: 新增 `tablePatternHasUnsupportedStar` 单元测试，覆盖部分 `*` 检测、映射目标侧 `*` 检测、合法 `db.*` 通配符映射等场景；同时补充 Oracle `dbms_stats.gather_table_stats` 相关回归测例。（issue #I6NPC1）
- [问题修复]: 修复 `tables` / `ignoreTables` 参数误用部分通配符 `*`（如 `sbtest.t*`）时静默产生错误结果的问题；现在会在参数校验阶段直接报错，并提示改用 `%`。
- [问题修复]: 修复表不存在时结果中的 `CheckObject` 被错误写成 `struct` 的问题；同时修复 `checkObject=struct` 模式下源端和目标端表都不存在时输出重复记录的问题。

## 1.2.5
- [功能新增]: 新增 `MariaDB -> MariaDB` 双端全模式支持（`data`/`struct`/`routine`/`trigger`），覆盖 `10.0`、`10.1`、`10.2`、`10.3`、`10.4`、`10.5`、`10.6`、`10.11`、`11.4`、`11.5`、`12.3` 系列，仅支持升级方向（src ≤ dst）；`struct` fix 隐藏索引使用 `IGNORED` 语法，`COMPRESSED`/`PERSISTENT` 等 MariaDB 原生列属性在目标端保留；各系列特性能力（JSON、不可见列、函数式索引、CHECK 约束强制执行等）按实际引入版本自动门控。
- [功能新增]: 新增 `MySQL 5.6/5.7/8.0/8.4` 同版本及升级链路支持，覆盖 `data`、`struct`、`routine`、`trigger` 四种校验模式；downgrade 或不支持的版本组合会在启动阶段直接拒绝执行。
- [功能新增]: 扩展 `MariaDB 10.x+ / 12.3+ -> MySQL 8.0/8.4` 支持至全部四种 `checkObject` 模式；新增参数 `mariaDBJSONTargetType` 支持 `MariaDB JSON` alias 改写为 `JSON`、`LONGTEXT` 或 `TEXT`；支持 `uca1400 -> uca0900` collation 自动映射，减少跨版本误报。
- [功能优化]: 统一结构语义比较与风险分级，将 `CHECK`、显示宽度、`utf8/utf8mb3`、`ZEROFILL`、`ROW_FORMAT`、默认 collation 漂移等差异收敛为 `warn-only` / `advisory-only` 分层输出；补齐 `routine` / `trigger` charset 元数据三维度
比对；`checkObject=data` 新增 DSN charset 一致性预检，DDL 差异表稳定保留并显示 `DDL-yes`。
- [测试完善]: 新增 `shouldCompareTriggerMetadata`/`shouldCompareRoutineMetadata` 单元测试 9 个，覆盖 MariaDB→MariaDB、MySQL→MariaDB、MySQL→MySQL、MariaDB→MySQL 8.0/8.4/5.7 各路径；新增 `BuildTargetColumnRepairPlan` 的 MariaDB→MariaDB 单元测试 4 个，覆盖 `COMPRESSED`/`PERSISTENT` 属性保留与剥除回归；补充 MariaDB→MariaDB 版本策略测试 15 个。
- [测试完善]: 新增 `tablePatternHasUnsupportedStar` 单元测试 6 个（`inputArg/checkParameter_test.go`），覆盖部分 `*` 检测、合法模式放行、映射目标侧 `*` 检测、双侧均含 `*`、合法 `db.*` 全量通配符映射、空字符串安全性。
- [测试完善]: MySQL 修复 SQL 生成路径新增 20 个单元测试，覆盖标识符引用（普通/含空格/含反引号/保留字）、ADD/DROP 索引、schema/table 名转义、外键名转义、routine/trigger DROP 转义、`normalizeAlterOperationContent` 正则提取等场
景；Oracle 修复 SQL 生成路径新增 12 个单元测试，覆盖 `oracleIdentifier` 语义（简单大写裸名、小写→大写、含空格加双引号、已引用保留、内部双引号转义）、DROP/ADD 索引 Oracle 语法正确性、`FixAlterIndexSqlGenerate` 原样透传等场景。
- [测试完善]: 新增 `EvaluateDataCheckPreflight` 回归测试 6 个（`actions/data_check_preflight_test.go`），覆盖源端表缺失、双端表缺失、空检查列表（Fatal）、有效表（Proceed）、混合有效/异常（Proceed）、invisible 列不匹配（SkipChecksum）等场景，防止 data 模式 preflight 回归。
- [问题修复]: 修复 `tables` / `ignoreTables` 参数使用不支持的部分通配符 `*`（如 `sbtest.t*`）时静默产生错误结果的问题；现在在参数校验阶段快速失败，打印明确提示信息（如 `use '%' instead, e.g. sbtest.t%`）并退出；同时覆盖映射目
标侧（如 `db1.t%:db2.t*` 中的 `db2.t*`）以及 `ignoreTables` 参数。
- [问题修复]: 修复 `checkObject=data` 或 `checkObject=struct` 模式下，当指定的表在源端或两端均不存在时，输出结果的 `CheckObject` 列被硬编码为 `struct` 而非用户实际配置值的问题；现在所有不存在表分支均正确输出用户配置的 `checkObject` 值。
- [问题修复]: 修复 `checkObject=struct` 模式下，当源端和目标端表均不存在时输出重复行的问题；根因为 `TableColumnNameCheck` 已将不存在的表追加到 pod 列表，而 `Struct()` 中的去重逻辑未感知这些 pod；修复方案为在调用 `TableColumnNameCheck` 前对 pod 快照，并将新增 pod 的表键预填充到去重集合中，防止重复创建。
- [问题修复]: 修复多类结构比较误报（`CHECK` 括号噪音、主键 canonical key 残余、映射场景目标表名错误、collation advisory 重复输出等），以及 `MySQL 5.6/5.7` 查询 `INFORMATION_SCHEMA.STATISTICS.IS_VISIBLE` 的低版本兼容问题和 `checkObject=data` DDL-yes 链路结果丢失问题。
- [问题修复]: 修复 DSN `charset` 参数提取不完整、`MariaDB` 源端全局权限预检查误判，以及 `struct` / `routine` / `trigger` 模式连接池过大导致的 `Too many connections` 问题（issue #IEYE7P）。
- [问题修复]: 修复 `checkObject=data` 模式下连接池大小不足导致数据校验 hang 住的问题：`data` 模式下 `queryTableDataSeparate` 与 `AbnormalDataDispos` 两条并发 pipeline 同时运行，单侧峰值连接需求约为 `parallelThds*2 + 2`；将单
侧连接池下限从 `parallelThds + 2` 调整为 `parallelThds*2 + 4`（最低 8），覆盖两阶段并发场景。
- [问题修复]: 修复连接池 `Get()` 持锁阻塞死锁及关闭竞态：`Get()` 原在持有 mutex 时阻塞等待 channel，导致 `Put()` 无法归还连接；同时 `Close()` 在 `Get()` 阻塞期间关闭 channel 后，`Get()` 会返回 nil 连接并错误递增计数。修复：将
 channel 等待移至 mutex 释放后，改用两值接收检测 channel 关闭，并将 `Close()` 中 `p.close = true` 移至 `close(p.conns)` 之前。
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
