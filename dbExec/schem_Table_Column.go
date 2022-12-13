package dbExec

import (
	"database/sql"
	"greatdbCheck/MySQL"
	"greatdbCheck/Oracle"
)

type TableColumnNameStruct struct {
	Schema  string
	Table   string
	Drive   string
	Db      *sql.DB
	Datafix string
}

type QueryTableColumnNameInterface interface {
	TableColumnName(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error)
	GlobalAccessPri(db *sql.DB, logThreadSeq int64) bool
	TableAccessPriCheck(db *sql.DB, checkTableList []string, datefix string, logThreadSeq int64) (map[string]int, error)
	DatabaseNameList(ignschema string, logThreadSeq int64) []string
	TableNameList(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error)
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
		aa = &mysql.QueryTable{
			Schema: tcns.Schema,
			Table:  tcns.Table,
			Db:     tcns.Db,
		}
	}
	if tcns.Drive == "godror" {
		aa = &oracle.QueryTable{
			Schema: tcns.Schema,
			Table:  tcns.Table,
			Db:     tcns.Db,
		}
	}
	return aa
}
