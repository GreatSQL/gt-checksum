package actions

import (
	"database/sql"
	"fmt"
	mysql "gt-checksum/MySQL"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"gt-checksum/schemacompat"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// 全局变量
var (
	// 用于存储表映射关系
	TableMappingRelations []string
)

const defaultFixSQLDir = "fixsql"

var partitionMetadataPattern = regexp.MustCompile(`^NAME=(.*?),ORDINAL=(.*?),METHOD=(.*?),EXPRESSION=(.*?),DESCRIPTION=(.*),ROWS=(.*)$`)
var partitionDelimiterSpacingPattern = regexp.MustCompile(`\s*([(),])\s*`)
var mysqlVersionedCommentWrapperPattern = regexp.MustCompile(`(?is)^/\*!\d+\s*(.*?)\s*\*/$`)
var partitionExpressionColumnPatternTemplate = `(^|[^A-Z0-9_])%s([^A-Z0-9_]|$)`

// routineHeaderIdentifierPattern 匹配 routine 定义头部的标识符（DEFINER、routine 名称），
// 用于仅对标识符做大小写归一，而不影响函数体中的字符串字面量。
var createTableTargetIdentifierPattern = regexp.MustCompile(`(?i)(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)` + `(?:(` + "`[^`]+`" + `)\.)?(` + "`[^`]+`" + `)`)

type partitionMetadata struct {
	Name        string
	Ordinal     int
	Method      string
	Expression  string
	Description string
	Rows        string
}

// measuredDataPods 在 terminal_result_output.go 中已定义

type schemaTable struct {
	// 现有字段...
	aggregate bool // 是否启用缓冲聚合（最小入侵新增）
	// 统一缓冲，用 CheckObject 区分 proc/func（最小入侵新增）
	podsBuffer              []Pod
	schema                  string
	table                   string
	rawTables               string // 原始 tables 参数，避免逐表处理时覆盖 table 后丢失 wildcard 语义
	destTable               string // 目标表名，可能与源表名不同
	ignoreSchema            string
	ignoreTable             string
	sourceDrive             string
	destDrive               string
	sourceVersion           global.MySQLVersionInfo
	destVersion             global.MySQLVersionInfo
	sourceDB                *sql.DB
	destDB                  *sql.DB
	caseSensitiveObjectName string
	datafix                 string
	datafixSql              string
	fixFileObjectType       string // 文件名中的对象类型前缀，如 "table"/"view"/"trigger"/"routine"
	djdbc                   string
	checkRules              inputArg.RulesS
	// 添加表映射规则
	tableMappings map[string]string
	// 需要跳过索引检查的表列表
	skipIndexCheckTables []string
	// 列修复操作映射表，用于合并列和索引操作
	columnRepairMap map[string][]string
	// Captures tables removed by ignoreTables for better diagnostics.
	ignoredMatchedTables []string
	// Keep per-run struct diff state on the schemaTable instance so repeated or
	// concurrent checks do not share mutable package-level maps.
	indexDiffsMap            map[string]bool
	partitionDiffsMap        map[string]bool
	foreignKeyDiffsMap       map[string]bool
	structWarnOnlyDiffsMap   map[string]bool
	structCollationMappedMap map[string]bool
	// objectKinds maps "schema/*schema&table*/table" (same key format as
	// DatabaseNameList) to the TABLE_TYPE value ("BASE TABLE" or "VIEW").
	// Populated once in SchemaTableFilter; absent key means "BASE TABLE".
	objectKinds map[string]string

	// columnPlan 非 nil 时表示当前运行处于部分列校验模式（columns 参数已配置）。
	// 在 TableColumnNameCheck 中用于豁免已明确映射的列对，避免误报 DDL mismatch。
	columnPlan *inputArg.TableColumnPlan

	// partitionsCache 缓存 Partitions() 结果，key=drive|schema|table（大小写敏感保留原值）。
	// 避免 struct 模式下同一张表在列校验与分区专项两个阶段重复查询。
	partitionsCache map[string]map[string]string

	// sourceOracleColumnsCache 批量预加载的 Oracle 源端列元数据（按 schema→table→rows）。
	// 仅在 Oracle 源端启用；一次 dba_tab_columns 扫描替代 N 次逐表 Q_table_columns。
	sourceOracleColumnsCache map[string]map[string][]map[string]interface{}

	// tableExistenceCache 按 "dbPtr|drive|SCHEMA" 预加载 schema 下所有 BASE TABLE 名称（统一大写）。
	// dbPtr 区分源/目标连接，避免同 drive+schema 场景下表名混淆。消除 tableExistsByDrive 的 COUNT(1) 小查询。
	tableExistenceCache map[string]map[string]struct{}

	// partitionedTableCache 按 "drive|SCHEMA" 预加载 schema 下「已分区」表名（统一大写）。
	// 仅 Oracle 侧启用；未列入的表视为非分区表，cachedPartitions 可直接返回空 map，
	// 跳过原逐表 all_tables COUNT(1) + DBMS_METADATA.GET_DDL 两次往返。
	partitionedTableCache map[string]map[string]struct{}
	// partitionedTableCacheLoaded 记录某 drive|schema 是否已完成批量预加载，避免重复建缓存。
	partitionedTableCacheLoaded map[string]bool
}

// rememberColumnRepairOperations defers column-level fix SQL until index repairs
// are known, so both kinds of changes can be emitted as one ALTER TABLE.

// adjustDestColumnSeqAfterDrops 删除 destColumnSeq 中 dropped 列的条目，
// 并将剩余列的序号向前压缩，消除因 DROP 列引起的位置偏移。
// 例如：目标端 my_row_id(pos=0)/f1(pos=1)/f2(pos=2)，DROP my_row_id 后
// f1→0、f2→1，与源端 f1(pos=0)/f2(pos=1) 保持一致，避免误判为序号不匹配。

func (stcls *schemaTable) sourceVersionInfo() global.MySQLVersionInfo {
	if stcls != nil && strings.TrimSpace(stcls.sourceVersion.Raw) != "" {
		return stcls.sourceVersion
	}
	return global.SourceMySQLVersion
}

func (stcls *schemaTable) destVersionInfo() global.MySQLVersionInfo {
	if stcls != nil && strings.TrimSpace(stcls.destVersion.Raw) != "" {
		return stcls.destVersion
	}
	return global.DestMySQLVersion
}

// normalizeStoredProcBody 规范化存储过程体，以便更准确地比较
// 规范化处理包括：
// 1. 移除多余的空格和换行符
// 2. 将所有空白字符规范化为单个空格
// 3. 移除注释
// 4. 将所有关键字转换为大写（可选，取决于数据库的大小写敏感性）
// 5. 规范化算术表达式，移除不必要的空格
// 如果不存在映射关系，返回格式为 "schema.table"

// getSourceTableName 返回源表的名称

// Explicit schema.table selections should win over ignoreTables to avoid
// silently dropping the only requested table from the checklist.

// getDestTableName 返回目标表的名称

/*
查询待校验表的列名
*/

// detectOracleToMySQLColumnHardMismatch 扫描 Oracle→MySQL 场景下同名列的
// 类型 / 字符集 / 排序规则，返回首个 hard-mismatch（非 WarnOnly、非 NormalizedEqual）
// 的列名与原因。用于 data 模式预检：当列名完全一致但底层定义不兼容时，
// 应像同构数据库一样将表标记为 DDL-yes，避免 data 模式反复生成同样的修复 SQL。
//
// 仅返回首个差异即可：对用户而言，只要存在任一 hard-mismatch，就必须先跑 struct
// 修复；不必枚举全部列（完整列表留给 struct 模式输出）。

// Routine metadata compare follows the same first-stage support matrix as
// trigger metadata: keep MySQL -> MySQL behavior unchanged, and explicitly
// enable MariaDB -> MySQL 8.0/8.4 so COMMENT and DEFINER drift are no longer
// silently skipped on the primary implementation path.

// Sequence objects are outside the table repair scope, so the best-effort
// behavior is to emit explicit warn-only rows and advisory notes up front.

var mysqlCreateObjectCommentPattern = regexp.MustCompile(`(?is)\bCOMMENT\s+'((?:\\'|[^'])*)'`)

// queryOraclePrimaryKeyColumns fetches the primary key column list (ordered by
// position) for an Oracle table from DBA_CONSTRAINTS/DBA_CONS_COLUMNS. The
// result is used to generate a MySQL CREATE TABLE PRIMARY KEY clause so the
// DDL is acceptable to MySQL 8.0+ instances with sql_require_primary_key=ON.

// tableExistsByDrive reports whether an object exists in the given database.
// objectKind: "" or "table" → require TABLE_TYPE='BASE TABLE';
//
//	"view"  → require TABLE_TYPE='VIEW'.
//
// Oracle only queries all_tables (views are ignored, objectKind has no effect).

// splitTableViewEntries partitions a dtabS slice into BASE-TABLE entries and VIEW entries.
// Entries whose source part maps to "VIEW" in objectKinds are placed in viewEntries;
// everything else (or when objectKinds is empty) goes to tableEntries.

/*
校验表的列名是否正确
*/

/*
该函数用于获取MySQL的表的索引信息,判断表是否存在索引，加入存在，获取索引的类型，以主键索引、唯一索引、普通索引及无索引，主键索引或唯一索引以自增id为优先

	缺少索引列为空或null的处理
*/

// 处理模糊匹配，支持数据库映射规则

/*
库表的所有列信息
*/

/*
获取校验表的索引列信息，包含是否有索引，列名，列序号
*/

// 解析表映射规则

/*
校验存储过程
*/
/*
最小入侵新增：统一附加与刷新方法
*/
func (stcls *schemaTable) setAggregate(on bool) {
	stcls.aggregate = on
}

func (stcls *schemaTable) appendPod(p Pod) {
	if stcls.aggregate {
		stcls.podsBuffer = append(stcls.podsBuffer, p)
	} else {
		measuredDataPods = append(measuredDataPods, p)
	}
}

func (stcls *schemaTable) flushPods() {
	if len(stcls.podsBuffer) > 0 {
		measuredDataPods = append(measuredDataPods, stcls.podsBuffer...)
		stcls.podsBuffer = nil
	}
}

/*
最小入侵新增：以返回值形式获取 Proc 结果
- 通过临时开启 aggregate 模式，复用现有 Proc 逻辑来采集 pods
- 调用结束后恢复原 aggregate 与 podsBuffer 状态
*/

/*
校验表结构是否正确
当设置checkObject=struct时，同时执行表结构、索引、分区和外键的校验
*/

/*
用于测试db链接串是否正确，是否可以连接
*/
func dbOpenTest(drive, jdbc string) *sql.DB {
	p := dbExec.DBexec()
	p.JDBC = jdbc
	p.DBDevice = drive
	db, err := p.OpenDB()
	if err != nil {
		fmt.Println("")
		os.Exit(1)
	}
	err1 := db.Ping()
	if err1 != nil {
		os.Exit(1)
	}
	return db
}

/*
库表的初始化
*/
func SchemaTableInit(m *inputArg.ConfigParameter) *schemaTable {
	sdb := dbOpenTest(m.SecondaryL.DsnsV.SrcDrive, m.SecondaryL.DsnsV.SrcJdbc)
	ddb := dbOpenTest(m.SecondaryL.DsnsV.DestDrive, m.SecondaryL.DsnsV.DestJdbc)

	// 加载用户自定义数据类型映射规则
	if path := strings.TrimSpace(m.SecondaryL.RulesV.DTypeMappingFile); path != "" {
		if err := schemacompat.LoadDTypeMappingFile(path); err != nil {
			global.Wlog.Warn(fmt.Sprintf("dTypeMapping: failed to load mapping file %q: %v", path, err))
		}
	}

	// 预览模式：输出规则表后退出
	if m.PreviewDTypeMapping {
		schemacompat.PrintDTypeMappingPreview()
		os.Exit(0)
	}

	// 初始化表映射关系
	tableMappings := make(map[string]string)

	// 解析tables参数中的映射关系
	tables := m.SecondaryL.SchemaV.Tables
	for _, tableItem := range strings.Split(tables, ",") {
		if strings.Contains(tableItem, ":") {
			parts := strings.Split(tableItem, ":")
			if len(parts) == 2 {
				// 处理db1.*:db2.*格式
				if strings.Contains(parts[0], ".*") && strings.Contains(parts[1], ".*") {
					sourceSchema := strings.TrimSuffix(parts[0], ".*")
					destSchema := strings.TrimSuffix(parts[1], ".*")
					tableMappings[sourceSchema] = destSchema
				} else {
					// 处理db1.table1:db2.table2格式
					sourceParts := strings.Split(parts[0], ".")
					destParts := strings.Split(parts[1], ".")
					if len(sourceParts) >= 1 && len(destParts) >= 1 {
						sourceSchema := sourceParts[0]
						destSchema := destParts[0]
						tableMappings[sourceSchema] = destSchema
					}
				}
			}
		}
	}

	// 添加调试日志
	vlog := fmt.Sprintf("Initialized table mappings: %v", tableMappings)
	global.Wlog.Debug(vlog)

	sourceVersion := global.SourceMySQLVersion
	if detectedVersion, err := queryVersionInfoFromDB(sdb); err == nil {
		sourceVersion = detectedVersion
	} else if strings.TrimSpace(sourceVersion.Raw) == "" {
		global.Wlog.Warn(fmt.Sprintf("SchemaTableInit failed to detect source database version from live connection: %v", err))
	}

	destVersion := global.DestMySQLVersion
	if detectedVersion, err := queryVersionInfoFromDB(ddb); err == nil {
		destVersion = detectedVersion
	} else if strings.TrimSpace(destVersion.Raw) == "" {
		global.Wlog.Warn(fmt.Sprintf("SchemaTableInit failed to detect target database version from live connection: %v", err))
	}

	return &schemaTable{
		ignoreTable:              m.SecondaryL.SchemaV.IgnoreTables,
		table:                    m.SecondaryL.SchemaV.Tables,
		rawTables:                m.SecondaryL.SchemaV.Tables,
		sourceDrive:              m.SecondaryL.DsnsV.SrcDrive,
		destDrive:                m.SecondaryL.DsnsV.DestDrive,
		sourceVersion:            sourceVersion,
		destVersion:              destVersion,
		sourceDB:                 sdb,
		destDB:                   ddb,
		caseSensitiveObjectName:  m.SecondaryL.SchemaV.CaseSensitiveObjectName,
		datafix:                  m.SecondaryL.RepairV.Datafix,
		datafixSql:               m.SecondaryL.RepairV.FixFileDir,
		djdbc:                    m.SecondaryL.DsnsV.DestJdbc,
		checkRules:               m.SecondaryL.RulesV,
		tableMappings:            tableMappings,
		columnRepairMap:          make(map[string][]string),
		indexDiffsMap:            make(map[string]bool),
		partitionDiffsMap:        make(map[string]bool),
		foreignKeyDiffsMap:       make(map[string]bool),
		structWarnOnlyDiffsMap:   make(map[string]bool),
		structCollationMappedMap: make(map[string]bool),
		columnPlan:               m.ColumnPlan,
	}
}

func (stcls *schemaTable) effectiveFixSQLDatafix(logThreadSeq int64) string {
	if strings.EqualFold(stcls.datafix, "table") && !strings.EqualFold(stcls.checkRules.CheckObject, "data") {
		if global.Wlog != nil {
			global.Wlog.Warn(fmt.Sprintf("(%d) checkObject=%s with datafix=table does not directly repair target objects; force exporting fix SQL file for manual review.", logThreadSeq, stcls.checkRules.CheckObject))
		}
		return "file"
	}
	return stcls.datafix
}

func (stcls *schemaTable) effectiveFixFileDir(logThreadSeq int64) string {
	fixFileDir := strings.TrimSpace(stcls.datafixSql)
	if fixFileDir != "" {
		return fixFileDir
	}

	if global.Wlog != nil {
		if absDir, err := filepath.Abs(defaultFixSQLDir); err == nil {
			global.Wlog.Info(fmt.Sprintf("(%d) fixFileDir is not set; using default fix SQL dir: %s", logThreadSeq, absDir))
		} else {
			global.Wlog.Info(fmt.Sprintf("(%d) fixFileDir is not set; using default fix SQL dir: %s", logThreadSeq, defaultFixSQLDir))
		}
	}
	return defaultFixSQLDir
}

func (stcls *schemaTable) objectFixFilePath(objType string, logThreadSeq int64) (string, error) {
	if objType == "" {
		objType = "table"
	}
	fixFileDir := stcls.effectiveFixFileDir(logThreadSeq)
	if err := os.MkdirAll(fixFileDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create fix file directory %s: %v", fixFileDir, err)
	}
	return filepath.Join(fixFileDir, fmt.Sprintf("%s.%s.%s.sql",
		objType,
		fixFileNameEncode(stcls.schema),
		fixFileNameEncode(stcls.table))), nil
}

/*
writeFixSql 处理修复SQL文件写入逻辑，每对象写入独立文件（type.schema.object.sql）
*/
func (stcls *schemaTable) writeFixSql(sqls []string, logThreadSeq int64) error {
	if len(sqls) == 0 {
		return nil
	}

	// Merge ALTER TABLE statements for the same table (including non-contiguous ones)
	// to reduce metadata lock overhead and shorten DDL execution time.
	sqls = mergeAlterTableStatements(sqls, logThreadSeq)
	effectiveDatafix := stcls.effectiveFixSQLDatafix(logThreadSeq)

	// 执行模式：仅 checkObject=data 允许直接在目标库执行。
	if strings.EqualFold(effectiveDatafix, "table") {
		if stcls.destDB == nil {
			err := fmt.Errorf("destination DB is nil in datafix=table mode")
			global.Wlog.Error(fmt.Sprintf("(%d) %v", logThreadSeq, err))
			return err
		}
		for _, raw := range sqls {
			stmt := normalizeFixSQLForExec(raw)
			if stmt == "" {
				continue
			}
			if _, err := stcls.destDB.Exec(stmt); err != nil {
				execErr := fmt.Errorf("failed to execute fix SQL in table mode: %v, sql: %s", err, stmt)
				global.Wlog.Error(fmt.Sprintf("(%d) %v", logThreadSeq, execErr))
				return execErr
			}
			global.Wlog.Debug(fmt.Sprintf("(%d) Executed fix SQL in table mode: %s", logThreadSeq, stmt))
		}
		return nil
	}

	// 预览模式：仅写入修复文件
	if !strings.EqualFold(effectiveDatafix, "file") {
		return nil
	}

	tableFileName, err := stcls.objectFixFilePath(stcls.fixFileObjectType, logThreadSeq)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(tableFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open fix file %s: %v", tableFileName, err)
	}
	defer file.Close()

	vlog := fmt.Sprintf("(%d) Opened object-specific fix file %s", logThreadSeq, tableFileName)
	global.Wlog.Debug(vlog)

	return mysql.WriteFixIfNeededFile(effectiveDatafix, file, sqls, logThreadSeq, stcls.djdbc)
}

/*
获取源数据库连接
*/
func (stcls *schemaTable) GetSourceDB() *sql.DB {
	return stcls.sourceDB
}

/*
获取目标数据库连接
*/
func (stcls *schemaTable) GetDestDB() *sql.DB {
	return stcls.destDB
}

// generateCreateTableSql 生成创建表的SQL语句，包括表级别的字符集和排序规则
// rewriteCreateTableTargetIdentifier rewrites the leading CREATE TABLE target
// only, so mapped-table repairs do not accidentally keep the source table name.
