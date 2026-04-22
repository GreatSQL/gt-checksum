package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"strings"
)

func normalizeTriggerActionStatement(statement string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(strings.TrimSpace(statement), "\n", " ")), " ")
}

func normalizeTriggerDefiner(definer string) string {
	return strings.TrimSpace(definer)
}

func buildTriggerCacheKey(schema, triggerName string) string {
	return strings.ToUpper(fmt.Sprintf("\"%s\".\"%s\"", schema, triggerName))
}

func buildTriggerCanonicalValue(actionTiming, eventManipulation, eventObjectTable, actionStatement string) string {
	return fmt.Sprintf(
		"%s %s %s %s",
		strings.ToUpper(strings.TrimSpace(actionTiming)),
		strings.ToUpper(strings.TrimSpace(eventManipulation)),
		strings.ToUpper(strings.TrimSpace(eventObjectTable)),
		normalizeTriggerActionStatement(actionStatement),
	)
}

func buildTriggerDefinerClause(definer string) string {
	normalized := normalizeTriggerDefiner(definer)
	if normalized == "" {
		return ""
	}

	atPos := strings.LastIndex(normalized, "@")
	if atPos <= 0 || atPos >= len(normalized)-1 {
		return ""
	}

	user := strings.Trim(normalized[:atPos], "`'\" ")
	host := strings.Trim(normalized[atPos+1:], "`'\" ")
	if user == "" || host == "" {
		return ""
	}

	user = strings.ReplaceAll(user, "`", "``")
	host = strings.ReplaceAll(host, "`", "``")
	return fmt.Sprintf("DEFINER=`%s`@`%s` ", user, host)
}

func BuildTriggerCreateSQL(schema, triggerName, definer, actionTiming, eventManipulation, eventObjectTable, actionStatement string) string {
	return fmt.Sprintf(
		"CREATE %sTRIGGER `%s`.`%s` %s %s ON `%s`.`%s` FOR EACH ROW %s",
		buildTriggerDefinerClause(definer),
		schema,
		triggerName,
		strings.ToUpper(strings.TrimSpace(actionTiming)),
		strings.ToUpper(strings.TrimSpace(eventManipulation)),
		schema,
		eventObjectTable,
		strings.TrimSpace(actionStatement),
	)
}

func (my *QueryTable) Trigger(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		tmpb   = make(map[string]string)
		Event  = "Q_Trigger"
		query  string
		logMsg string
		err    error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the trigger information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf(
		"SELECT TRIGGER_NAME AS TRIGGER_NAME, ACTION_TIMING AS ACTION_TIMING, EVENT_MANIPULATION AS EVENT_MANIPULATION, EVENT_OBJECT_TABLE AS EVENT_OBJECT_TABLE, ACTION_STATEMENT AS ACTION_STATEMENT FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA IN('%s');",
		my.Schema,
	)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	triggerRows, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, row := range triggerRows {
		triggerName := fmt.Sprintf("%s", row["TRIGGER_NAME"])
		actionTiming := fmt.Sprintf("%s", row["ACTION_TIMING"])
		eventManipulation := fmt.Sprintf("%s", row["EVENT_MANIPULATION"])
		eventObjectTable := fmt.Sprintf("%s", row["EVENT_OBJECT_TABLE"])
		actionStatement := fmt.Sprintf("%s", row["ACTION_STATEMENT"])

		// INFORMATION_SCHEMA.TRIGGERS already exposes canonical trigger
		// metadata, which is safer than parsing SHOW CREATE TRIGGER output.
		tmpb[buildTriggerCacheKey(my.Schema, triggerName)] = buildTriggerCanonicalValue(
			actionTiming,
			eventManipulation,
			eventObjectTable,
			actionStatement,
		)
		logMsg = fmt.Sprintf("(%d) [%s] Stored trigger %s.%s canonical definition", logThreadSeq, Event, my.Schema, triggerName)
		global.Wlog.Debug(logMsg)
	}
	logMsg = fmt.Sprintf("(%d) [%s] Complete the trigger information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return tmpb, nil
}

/*
MySQL 存储过程和函数统一校验（新增）
- 一次性从 INFORMATION_SCHEMA.PARAMETERS 与 INFORMATION_SCHEMA.ROUTINES 查询
- 按 ROUTINE_TYPE 将结果分别组装为 PROCEDURE / FUNCTION 的定义文本
- 返回 routines 与 types 两张表，供上层或兼容包装使用
*/
func (my *QueryTable) Routine(db *sql.DB, logThreadSeq int64) (map[string]string, map[string]string, error) {
	var (
		routines = make(map[string]string) // name -> body
		types    = make(map[string]string) // name -> "PROCEDURE"/"FUNCTION"
		Event    = "Q_Routine"
		query    string
		logMsg   string
		err      error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query PROCEDURE and FUNCTION information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)

	// 1) 查询参数：同时取 PROCEDURE 与 FUNCTION
	query = fmt.Sprintf("SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME, ROUTINE_TYPE, ORDINAL_POSITION, PARAMETER_MODE, PARAMETER_NAME, DTD_IDENTIFIER FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_SCHEMA IN('%s') AND ROUTINE_TYPE IN('PROCEDURE','FUNCTION') ORDER BY SPECIFIC_NAME, ORDINAL_POSITION;", my.Schema)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, nil, err
	}
	inoutAll, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, nil, err
	}

	// 拆分参数到 Proc/Func 两组
	var inoutProc, inoutFunc []map[string]interface{}
	for _, r := range inoutAll {
		if strings.EqualFold(fmt.Sprintf("%s", r["ROUTINE_TYPE"]), "PROCEDURE") {
			inoutProc = append(inoutProc, r)
		} else if strings.EqualFold(fmt.Sprintf("%s", r["ROUTINE_TYPE"]), "FUNCTION") {
			inoutFunc = append(inoutFunc, r)
		}
	}
	tmpaProc := procP(inoutProc, "Proc")
	tmpaFunc := procP(inoutFunc, "Func")

	// 2) 从 ROUTINES 取定义与属性，并带出 ROUTINE_TYPE
	query = fmt.Sprintf("SELECT ROUTINE_NAME, ROUTINE_DEFINITION, DEFINER, SQL_MODE, CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION, ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA='%s' AND ROUTINE_TYPE IN('PROCEDURE','FUNCTION');", my.Schema)
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, nil, err
	}
	createAll, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, nil, err
	}
	defer dispos.SqlRows.Close()

	// 拆分 ROUTINES 到 Proc/Func 两组，并用现有 procR 生成定义
	var createProc, createFunc []map[string]interface{}
	for _, r := range createAll {
		if strings.EqualFold(fmt.Sprintf("%s", r["ROUTINE_TYPE"]), "PROCEDURE") {
			createProc = append(createProc, r)
		} else if strings.EqualFold(fmt.Sprintf("%s", r["ROUTINE_TYPE"]), "FUNCTION") {
			createFunc = append(createFunc, r)
		}
	}

	procMap := procR(createProc, tmpaProc, "Proc")
	funcMap := procR(createFunc, tmpaFunc, "Func")

	// 合并并记录类型
	for k, v := range procMap {
		routines[k] = v
		types[k] = "PROCEDURE"
	}
	for k, v := range funcMap {
		routines[k] = v
		types[k] = "FUNCTION"
	}

	logMsg = fmt.Sprintf("(%d) [%s] Complete the PROCEDURE and FUNCTION information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	return routines, types, nil
}

/*
MySQL 存储过程校验
*/
/*
Deprecated: use Routine() instead.
兼容包装：复用 Routine()，仅返回 PROCEDURE。
*/
func (my *QueryTable) Proc(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	routines, types, err := my.Routine(db, logThreadSeq)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for name, body := range routines {
		if strings.EqualFold(types[name], "PROCEDURE") {
			out[name] = body
		}
	}
	return out, nil
}

/*
MySQL 存储函数或自定义函数校验
*/
/*
Deprecated: use Routine() instead.
兼容包装：复用 Routine()，仅返回 FUNCTION。
*/
func (my *QueryTable) Func(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	routines, types, err := my.Routine(db, logThreadSeq)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for name, body := range routines {
		if strings.EqualFold(types[name], "FUNCTION") {
			out[name] = body
		}
	}
	return out, nil
}

/*
MySQL 外键校验
*/
