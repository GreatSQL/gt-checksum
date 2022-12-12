package dbExec

import (
	"database/sql"
	mysql "greatdbCheck/MySQL"
	oracle "greatdbCheck/Oracle"
)

type DataAbnormalFixStruct struct {
	Schema          string
	Table           string
	RowData         string
	SourceDevice    string
	DestDevice      string
	Sqlwhere        string
	IndexColumnType string
	ColData         []map[string]string
}
type DataAbnormalFixInterface interface {
	FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error)
	FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error)
}

func (dafs DataAbnormalFixStruct) DataAbnormalFix() DataAbnormalFixInterface {
	var tqaci DataAbnormalFixInterface
	if dafs.SourceDevice == "mysql" {
		tqaci = &mysql.MysqlDataAbnormalFixStruct{
			Schema:          dafs.Schema,
			Table:           dafs.Table,
			Sqlwhere:        dafs.Sqlwhere,
			RowData:         dafs.RowData,
			SourceDevice:    dafs.SourceDevice,
			IndexColumnType: dafs.IndexColumnType,
			ColData:         dafs.ColData,
		}
	}
	if dafs.SourceDevice == "godror" {
		tqaci = &oracle.OracleDataAbnormalFixStruct{
			Schema:          dafs.Schema,
			Table:           dafs.Table,
			Sqlwhere:        dafs.Sqlwhere,
			RowData:         dafs.RowData,
			SourceDevice:    dafs.SourceDevice,
			IndexColumnType: dafs.IndexColumnType,
			ColData:         dafs.ColData,
		}
	}
	return tqaci
}
func DataFix() *DataAbnormalFixStruct {
	return &DataAbnormalFixStruct{}
}
