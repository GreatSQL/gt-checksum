package dbExec

import (
	"database/sql"
	mysql "gt-checksum/MySQL"
	"strings"
)

// IndexColumnStruct carries metadata required to build index-based query chunks.
type IndexColumnStruct struct {
	Drivce                  string
	Schema                  string
	Table                   string
	CaseSensitiveObjectName string
	TmpTableFileName        string
	ColumnName              []string
	ChanrowCount            int
	TableColumn             []map[string]string
	Sqlwhere                string
	ColData                 []map[string]string
	BeginSeq                string
	RowDataCh               int64
	SelectColumn            map[string]string
	// CompareColumns, when non-empty, restricts the SELECT column list to only the
	// named columns (in the order specified). Used by the columns partial-compare mode.
	// Source and target sides carry their own respective column name lists.
	CompareColumns []string
}

type TableIndexColumner interface {
	TmpTableIndexColumnSelectDispos(logThreadSeq int64) map[string]string
	TmpTableIndexColumnRowsCount(db *sql.DB, logThreadSeq int64) (uint64, error)
	TmpTableColumnGroupDataDispos(db *sql.DB, where string, columnName string, logThreadSeq int64) (chan map[string]interface{}, error)
	TableRows(db *sql.DB, logThreadSeq int64) (uint64, error)
	TableComment(db *sql.DB, logThreadSeq int64) (string, error)
	QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error)
	IndexDisposF(queryData []map[string]interface{}, logThreadSeq int64) (map[string][]string, map[string][]string, map[string][]string, map[string]string)
	NoIndexOrderBySingerColumn(orderCol []map[string]string) []string
	NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq uint64, chanrowCount int, logThreadSeq int64) (string, error)
	GeneratingQuerySql(db *sql.DB, logThreadSeq int64) (string, error)
	GeneratingQueryCriteria(db *sql.DB, logThreadSeq int64) (string, error)
}

func (qticis *IndexColumnStruct) TableIndexColumn() TableIndexColumner {
	var aa TableIndexColumner
	if qticis.Drivce == "mysql" {
		schemaName := qticis.Schema
		tableName := qticis.Table
		if strings.ToLower(qticis.CaseSensitiveObjectName) == "no" {
			schemaName = strings.ToLower(schemaName)
			tableName = strings.ToLower(tableName)
		}
		aa = &mysql.QueryTable{
			Schema:                  schemaName,
			Table:                   tableName,
			CaseSensitiveObjectName: qticis.CaseSensitiveObjectName,
			ColumnName:              qticis.ColumnName,
			ChanrowCount:            qticis.ChanrowCount,
			TableColumn:             qticis.TableColumn,
			Sqlwhere:                qticis.Sqlwhere,
			ColData:                 qticis.ColData,
			SelectColumn:            qticis.SelectColumn,
			BeginSeq:                qticis.BeginSeq,
			RowDataCh:               qticis.RowDataCh,
			CompareColumns:          qticis.CompareColumns,
		}
	}
	if qticis.Drivce == "godror" {
		aa = newOracleTableIndexColumner(qticis)
	}
	return aa
}
