package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"strings"
	"sync"
)

type QueryTable struct {
	Schema                  string
	Table                   string
	IgnoreTable             string
	Db                      *sql.DB
	Datafix                 string
	CaseSensitiveObjectName string
	// CandidateSchemas, when non-empty, restricts ObjectTypeMap to query only
	// the listed schemas instead of all non-system schemas. Used by
	// SchemaTableFilter to avoid a full INFORMATION_SCHEMA scan.
	CandidateSchemas        []string
	TmpTableFileName        string
	ColumnName              []string
	ChanrowCount            int
	TableColumn             []map[string]string
	Sqlwhere                string
	ColData                 []map[string]string
	BeginSeq                string
	RowDataCh               int64
	SelectColumn            map[string]string
	// Caching fields to optimize repeated INFORMATION_SCHEMA queries
	columnExistsCache   map[string]bool   // Cache for column existence checks
	allColumnsCache     []string          // Cache for all column names ordered by ORDINAL_POSITION
	columnDataTypeCache map[string]string // Cache for column name to data type mapping

	// CompareColumns, when non-empty, restricts the SELECT column list produced by
	// GeneratingQuerySql to only the named columns (in the specified order).
	// Set by the columns partial-compare mode; source and target QueryTable instances
	// carry their own respective lists (SourceColumns / TargetColumns from TableColumnPlan).
	CompareColumns []string
}

var (
	DBType = "MySQL"

	// Global caching for expensive INFORMATION_SCHEMA queries
	// These caches are shared across all QueryTable instances
	// cache key format: schema.table.column for column existence
	// cache key format: schema.table for column lists and data types
	columnExistsGlobalCache   = make(map[string]bool)
	allColumnsGlobalCache     = make(map[string][]string)
	columnDataTypeGlobalCache = make(map[string]string)
	tableColumnGlobalCache    = make(map[string][]map[string]string)      // Cache for complete table column information (fills TableColumn field)
	tableAllColumnGlobalCache = make(map[string][]map[string]interface{}) // Cache for TableAllColumn results
	// Cache for database version information (fills SELECT VERSION() requests)
	// Cache key format: connection identifier
	databaseVersionCache = make(map[string]string)
	// Mutex to protect global caches
	cacheMutex sync.RWMutex

	procP = func(inout []map[string]interface{}, event string) map[string]string {
		var tmpa = make(map[string][]string)
		for _, v := range inout {
			specificName := fmt.Sprintf("%s", v["SPECIFIC_NAME"])
			parameterName := strings.TrimSpace(fmt.Sprintf("%v", v["PARAMETER_NAME"]))
			if parameterName == "" || strings.EqualFold(parameterName, "<nil>") || strings.EqualFold(parameterName, "NULL") {
				continue
			}

			parameterMode := strings.TrimSpace(fmt.Sprintf("%s", v["PARAMETER_MODE"]))
			dtdIdentifier := strings.TrimSpace(fmt.Sprintf("%s", v["DTD_IDENTIFIER"]))
			if dtdIdentifier == "" || strings.EqualFold(dtdIdentifier, "<nil>") || strings.EqualFold(dtdIdentifier, "NULL") {
				continue
			}

			segment := fmt.Sprintf("%s %s", parameterName, dtdIdentifier)
			if event == "Func" {
				tmpa[specificName] = append(tmpa[specificName], segment)
				continue
			}

			if parameterMode != "" && !strings.EqualFold(parameterMode, "<nil>") && !strings.EqualFold(parameterMode, "NULL") {
				segment = fmt.Sprintf("%s %s", parameterMode, segment)
			}
			tmpa[specificName] = append(tmpa[specificName], segment)
		}

		result := make(map[string]string, len(tmpa))
		for specificName, params := range tmpa {
			result[specificName] = strings.Join(params, ", ")
		}
		return result
	}
	procR = func(createProc []map[string]interface{}, tmpa map[string]string, event string) map[string]string {
		var tmpb = make(map[string]string)
		lookupParamSignature := func(routineName string) string {
			if signature, ok := tmpa[routineName]; ok {
				return signature
			}
			for name, signature := range tmpa {
				if strings.EqualFold(name, routineName) {
					return signature
				}
			}
			return ""
		}
		splitDefiner := func(definer string) (string, string) {
			normalized := strings.TrimSpace(definer)
			if normalized == "" || strings.EqualFold(normalized, "<entry>") || strings.EqualFold(normalized, "<nil>") {
				return "", ""
			}
			atPos := strings.LastIndex(normalized, "@")
			if atPos <= 0 || atPos >= len(normalized)-1 {
				return strings.Trim(normalized, "`'\" "), ""
			}
			user := strings.Trim(normalized[:atPos], "`'\" ")
			host := strings.Trim(normalized[atPos+1:], "`'\" ")
			return user, host
		}

		for _, v := range createProc {
			routineDefinition := strings.TrimSpace(fmt.Sprintf("%s", v["ROUTINE_DEFINITION"]))
			routineName := strings.TrimSpace(fmt.Sprintf("%s", v["ROUTINE_NAME"]))
			if routineName == "" || strings.EqualFold(routineName, "<entry>") || strings.EqualFold(routineName, "<nil>") {
				continue
			}

			sqlMode := fmt.Sprintf("%s", v["SQL_MODE"])
			charsetClient := fmt.Sprintf("%s", v["CHARACTER_SET_CLIENT"])
			collationConn := fmt.Sprintf("%s", v["COLLATION_CONNECTION"])
			dbCollation := fmt.Sprintf("%s", v["DATABASE_COLLATION"])
			definer := fmt.Sprintf("%s", v["DEFINER"])
			user, host := splitDefiner(definer)

			definerClause := ""
			if user != "" && host != "" {
				definerClause = fmt.Sprintf("DEFINER='%s'@'%s' ", user, host)
			}
			routineIdentifier := fmt.Sprintf("`%s`", strings.ReplaceAll(routineName, "`", "``"))
			paramSignature := lookupParamSignature(routineName)

			// 将存储过程的完整定义和属性存储在一个JSON格式的字符串中
			if event == "Proc" {
				// 创建一个包含所有属性的JSON格式字符串，并将其嵌入到存储过程定义中
				// 使用特殊注释格式 /*GT_CHECKSUM_METADATA:...*/，这样不会影响存储过程的执行
				metadataComment := fmt.Sprintf(`/*GT_CHECKSUM_METADATA:{"sql_mode":"%s","character_set_client":"%s","collation_connection":"%s","database_collation":"%s","definer":"%s"}*/`,
					sqlMode, charsetClient, collationConn, dbCollation, definer)

				createStmt := fmt.Sprintf("CREATE %sPROCEDURE %s(%s)", definerClause, routineIdentifier, paramSignature)
				if routineDefinition != "" && !strings.EqualFold(routineDefinition, "<entry>") && !strings.EqualFold(routineDefinition, "<nil>") {
					createStmt = fmt.Sprintf("%s %s", createStmt, routineDefinition)
				}
				tmpb[routineName] = fmt.Sprintf("%s\n%s", metadataComment, createStmt)
				tmpb[routineName+"_BODY"] = routineDefinition
			}

			if event == "Func" {
				// 创建一个包含所有属性的JSON格式字符串，并将其嵌入到函数定义中
				metadataComment := fmt.Sprintf(`/*GT_CHECKSUM_METADATA:{"sql_mode":"%s","character_set_client":"%s","collation_connection":"%s","database_collation":"%s","definer":"%s"}*/`,
					sqlMode, charsetClient, collationConn, dbCollation, definer)

				createStmt := fmt.Sprintf("CREATE %sFUNCTION %s(%s)", definerClause, routineIdentifier, paramSignature)
				if routineDefinition != "" && !strings.EqualFold(routineDefinition, "<entry>") && !strings.EqualFold(routineDefinition, "<nil>") {
					createStmt = fmt.Sprintf("%s %s", createStmt, routineDefinition)
				}
				tmpb[routineName] = fmt.Sprintf("%s\n%s", metadataComment, createStmt)
			}
		}
		return tmpb
	}
)

func (my *QueryTable) TableColumnName(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event  = "Q_table_columns"
		query  string
		logMsg string
		err    error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start querying the metadata information of table %s.%s in the %s database and get all the column names", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT COLUMN_NAME AS columnName, COLUMN_TYPE AS columnType, IS_NULLABLE AS isNull, CHARACTER_SET_NAME AS charset, COLLATION_NAME AS collationName, COLUMN_COMMENT AS columnComment, COLUMN_DEFAULT AS columnDefault, EXTRA AS extra FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	logMsg = fmt.Sprintf("(%d) [%s] Complete the acquisition of all column names in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
MySQL 获取表的注释信息
*/
func (my *QueryTable) TableComment(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		Event  = "Q_Table_Comment"
		query  string
		logMsg string
		err    error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the comment of table %s.%s in the %s database", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT TABLE_COMMENT AS tableComment FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return "", err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return "", err
	}

	comment := ""
	if len(tableData) > 0 {
		comment = fmt.Sprintf("%s", tableData[0]["tableComment"])
	}

	logMsg = fmt.Sprintf("(%d) [%s] Complete the comment query of table %s.%s in the %s database: %s", logThreadSeq, Event, my.Schema, my.Table, DBType, comment)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return comment, nil
}

/*
MySQL 查询数据库版本信息
*/
func (my *QueryTable) TableAllColumn(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event    = "Q_Table_Column_Metadata"
		err      error
		query    string
		logMsg   string
		cacheKey string
	)

	cacheKey = scopedTableCacheKey(db, my.Schema, my.Table, "tableAllColumn")

	// Check if result is already in global cache
	cacheMutex.RLock()
	if cachedTableAllColumn, ok := tableAllColumnGlobalCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		logMsg = fmt.Sprintf("(%d) [%s] Using cached TableAllColumn information for table %s.%s", logThreadSeq, Event, my.Schema, my.Table)
		global.Wlog.Debug(logMsg)
		return cachedTableAllColumn, nil
	}
	cacheMutex.RUnlock()

	logMsg = fmt.Sprintf("(%d) [%s] Start to query the metadata of all the columns of table %s.%s in the %s database", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT COLUMN_NAME AS columnName, COLUMN_TYPE AS dataType, ORDINAL_POSITION AS columnSeq, IS_NULLABLE AS isNull, COLUMN_COMMENT AS columnComment, EXTRA AS extra FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION;", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	// Cache the result in global cache for future use
	cacheMutex.Lock()
	tableAllColumnGlobalCache[cacheKey] = tableData
	cacheMutex.Unlock()

	logMsg = fmt.Sprintf("(%d) [%s] Complete the metadata query of all columns in table %s.%s in the %s database. Cached results for future use.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
MySQL 处理唯一索引索引（包含主键索引）
*/
func (my *QueryTable) keyChoiceDispos(IndexColumnMap map[string][]string, indexType string) map[string][]string {
	var (
		a, c                 = make(map[string][]string), make(map[string][]int)
		indexChoice          = make(map[string][]string)
		breakIndexColumnType = []string{"INT", "FLOAT", "DOUBLE", "DECIMAL", "CHAR", "VARCHAR", "YEAR", "DATE", "TIME"}
		tmpSliceNum          = 100
		tmpSliceNumMap       = make(map[string]int)
		z                    string
		choseSeq             = 1000000
		intCharMax           int
		indexChoisName       string
	)
	// ----- 处理唯一索引列，根据选择规则选择一个单列索引，（选择次序：int<--char<--year<--date<-time<-其他）
	//先找出唯一联合索引数量最少的
	for k, i := range IndexColumnMap {
		if len(i) <= tmpSliceNum {
			if len(i) < tmpSliceNum {
				delete(tmpSliceNumMap, z)
			}
			tmpSliceNum = len(i)
			tmpSliceNumMap[k] = len(i)
			z = k
		}
	}
	//单列唯一索引处理，选择最短的且最合适的索引列（选择次序：int<--char<--year<--date<-time）
	for k, v := range tmpSliceNumMap {
		if v == 1 {
			d := strings.Split(strings.Join(IndexColumnMap[k], ""), " /*actions Column Type*/ ")
			indexColType := d[1]
			var e []string
			for kb, vb := range breakIndexColumnType {
				if strings.Contains(strings.ToUpper(indexColType), vb) {
					if kb < choseSeq {
						indexChoice[fmt.Sprintf("%s_single", indexType)] = append(e, d[0])
					}
					choseSeq = kb
				}
			}
		}
		if v > 1 {
			var nultIndexColumnSlice, nultIndexColumnTypeSlice []string
			for _, vu := range IndexColumnMap[k] {
				e := strings.Split(vu, " /*actions Column Type*/ ")
				nultIndexColumnSlice = append(nultIndexColumnSlice, e[0])
				nultIndexColumnTypeSlice = append(nultIndexColumnTypeSlice, e[1])
			}
			tmpIntCount := strings.Count(strings.ToUpper(strings.Join(nultIndexColumnTypeSlice, ",")), "INT")
			tmpCharCount := strings.Count(strings.ToUpper(strings.Join(nultIndexColumnTypeSlice, ",")), "CHAR")
			//处理索引列数量相同的情况，计算每个索引列中包含的int和char数量
			c[k] = []int{tmpIntCount, tmpCharCount}
			a[k] = nultIndexColumnSlice
		}
	}

	for k, v := range c {
		if v[0] > intCharMax {
			intCharMax = v[0]
			indexChoisName = k
		}
		if indexChoisName == "" && intCharMax == 0 && v[1] > 0 {
			intCharMax = v[0]
			indexChoisName = k
		}
		if v[0] == 0 && v[1] == 0 {
			indexChoisName = k
			break
		}
	}
	indexChoice[fmt.Sprintf("%s_multiseriate", indexType)] = a[indexChoisName]
	return indexChoice
}

/*
MySQL 表的索引选择
*/
func (my *QueryTable) TableIndexChoice(queryData []map[string]interface{}, logThreadSeq int64) map[string][]string {
	var (
		indexChoice                           = make(map[string][]string)
		nultiseriateIndexColumnMap            = make(map[string][]string)
		multiseriateIndexColumnMap            = make(map[string][]string)
		PriIndexCol, uniIndexCol, mulIndexCol []string
		indexName                             string
		Event                                 = "Q_Table_Index_Choice"
		logMsg                                string
	)
	if len(queryData) == 0 {
		return nil
	}
	//索引列处理，联合索引进行列合并
	//去除主键索引列、唯一索引列、普通索引列的所有列明
	logMsg = fmt.Sprintf("(%d) [%s] Start to select the appropriate index column in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	for _, v := range queryData {
		if v["nonUnique"].(string) == "0" {
			//处理主键索引
			if strings.Contains(v["indexName"].(string), "PRIMARY") {
				if v["indexName"].(string) != indexName {
					indexName = v["indexName"].(string)
				}
				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", v["columnName"]))
			}
			//处理唯一索引
			if v["indexName"].(string) != indexName {
				indexName = v["indexName"].(string)
				nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			} else {
				nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			}
		}
		//处理普通索引
		if v["nonUnique"].(string) == "1" {
			if v["indexName"].(string) != indexName {
				indexName = v["indexName"].(string)
				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			} else {
				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			}
		}
	}
	//vlog = fmt.Sprintf("(%d) MySQL DB index merge processing complete. The index merged data is {primary key: %v,unique key: %v,nounique key: %v}", logThreadSeq, PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap)
	//global.Wlog.Debug(vlog)
	//处理主键索引列
	//判断是否存在主键索引,每个表的索引只有一个
	logMsg = fmt.Sprintf("(%d) MySQL DB primary key index starts to choose the best.", logThreadSeq)
	global.Wlog.Debug(logMsg)
	if len(PriIndexCol) == 1 { //单列主键索引
		indexChoice["pri_single"] = PriIndexCol
	} else if len(PriIndexCol) > 1 { //联合主键索引
		indexChoice["pri_multiseriate"] = PriIndexCol
	}
	logMsg = fmt.Sprintf("(%d) MySQL DB unique key index starts to choose the best.", logThreadSeq)
	global.Wlog.Debug(logMsg)
	g := my.keyChoiceDispos(nultiseriateIndexColumnMap, "uni")
	for k, v := range g {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	f := my.keyChoiceDispos(multiseriateIndexColumnMap, "mul")
	for k, v := range f {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	logMsg = fmt.Sprintf("(%d) [%s] Complete the selection of the appropriate index column in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	return indexChoice
}

/*
MySQL 查询触发器信息
*/
