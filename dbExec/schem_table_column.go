package dbExec

import (
	"database/sql"
	mysql "gt-checksum/MySQL"
	"strings"
)

type TableColumnNameStruct struct {
	Schema                  string
	Table                   string
	IgnoreTable             string
	Drive                   string
	Db                      *sql.DB
	Datafix                 string
	CaseSensitiveObjectName string
	// CandidateSchemas, when non-empty, is forwarded to the driver's
	// ObjectTypeMap implementation so it can restrict the metadata query
	// to only the schemas that are relevant for this run.
	CandidateSchemas        []string
}

type QueryTableColumnNameInterface interface {
	TableColumnName(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error)
	GlobalAccessPri(db *sql.DB, logThreadSeq int64) (bool, error)
	TableAccessPriCheck(db *sql.DB, checkTableList []string, datafix string, logThreadSeq int64) (map[string]int, error)
	DatabaseNameList(db *sql.DB, logThreadSeq int64) (map[string]int, error)
	// ObjectTypeMap returns a mapping from canonical object key
	// ("schema/*schema&table*/table") to TABLE_TYPE ("BASE TABLE" or "VIEW").
	// Implementations for drivers that do not support views (e.g. Oracle) may
	// return an empty map; callers must treat an absent key as "BASE TABLE".
	ObjectTypeMap(db *sql.DB, logThreadSeq int64) (map[string]string, error)
	TableAllColumn(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error)
	TableIndexChoice(queryData []map[string]interface{}, logThreadSeq int64) map[string][]string
	Trigger(db *sql.DB, logThreadSeq int64) (map[string]string, error)
	Proc(db *sql.DB, logThreadSeq int64) (map[string]string, error)
	Func(db *sql.DB, logThreadSeq int64) (map[string]string, error)
	Struct(db *sql.DB) (map[string]string, error)
	Foreign(db *sql.DB, logThreadSeq int64) (map[string]string, error)
	Partitions(db *sql.DB, logThreadSeq int64) (map[string]string, error)
}

func (tcns *TableColumnNameStruct) Query() QueryTableColumnNameInterface {
	var aa QueryTableColumnNameInterface
	if tcns.Drive == "mysql" {
		schemaName := tcns.Schema
		tableName := tcns.Table
		if strings.ToLower(tcns.CaseSensitiveObjectName) == "no" {
			schemaName = strings.ToLower(schemaName)
			tableName = strings.ToLower(tableName)
		}
		aa = &mysql.QueryTable{
			Schema:                  schemaName,
			Table:                   tableName,
			Db:                      tcns.Db,
			CaseSensitiveObjectName: tcns.CaseSensitiveObjectName,
			CandidateSchemas:        tcns.CandidateSchemas,
		}
	}
	if tcns.Drive == "godror" {
		aa = newOracleTableColumnQuery(tcns)
	}
	return aa
}
