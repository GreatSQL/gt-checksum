package dbExec

import (
	"database/sql"
	mysql "greatdbCheck/MySQL"
	oracle "greatdbCheck/Oracle"
)

type IndexColumnStruct struct {
	Drivce           string
	Schema           string
	Table            string
	TmpTableFileName string
	ColumnName       []string
	ChanrowCount     int
	TableColumn      []map[string]string
	//TableColumn global.TableAllColumnInfoS
	Sqlwhere                       string
	ColData                        []map[string]string
	SelectColumnString, LengthTrim string
	ColumnLengthAs                 []string
	BeginSeq                       string
	RowDataCh                      int64
}

type TableIndexColumner interface {
	QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error)
	IndexDisposF(queryData []map[string]interface{}, logThreadSeq int64) ([]string, map[string][]string, map[string][]string)
	TmpTableRowsCount(db *sql.DB, logThreadSeq int64) (int, error)
	TmpTableIndexColumnDataDispos(db *sql.DB, logThreadSeq int64) ([]string, error)
	TmpTableIndexColumnDataLength(logThreadSeq int64) (string, []string, string)
	NoIndexOrderBySingerColumn(orderCol []map[string]string) string
	NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq, chanrowCount int, orderCol string, logThreadSeq int64) (string, error)
	GeneratingQuerySql(logThreadSeq int64) string
	GeneratingQueryCriteria(db *sql.DB, logThreadSeq int64) (string, error)
}

func (qticis *IndexColumnStruct) TableIndexColumn() TableIndexColumner {
	var aa TableIndexColumner
	if qticis.Drivce == "mysql" {
		aa = &mysql.QueryTableDate{
			Schema:             qticis.Schema,
			Table:              qticis.Table,
			ColumnName:         qticis.ColumnName,
			ChanrowCount:       qticis.ChanrowCount,
			TableColumn:        qticis.TableColumn,
			Sqlwhere:           qticis.Sqlwhere,
			ColData:            qticis.ColData,
			SelectColumnString: qticis.SelectColumnString,
			LengthTrim:         qticis.LengthTrim,
			ColumnLengthAs:     qticis.ColumnLengthAs,
			BeginSeq:           qticis.BeginSeq,
			RowDataCh:          qticis.RowDataCh,
		}
	}
	if qticis.Drivce == "godror" {
		aa = &oracle.QueryTableDate{
			Schema:             qticis.Schema,
			Table:              qticis.Table,
			ColumnName:         qticis.ColumnName,
			ChanrowCount:       qticis.ChanrowCount,
			TableColumn:        qticis.TableColumn,
			Sqlwhere:           qticis.Sqlwhere,
			ColData:            qticis.ColData,
			SelectColumnString: qticis.SelectColumnString,
			LengthTrim:         qticis.LengthTrim,
			ColumnLengthAs:     qticis.ColumnLengthAs,
			BeginSeq:           qticis.BeginSeq,
			RowDataCh:          qticis.RowDataCh,
		}
	}
	return aa
}
