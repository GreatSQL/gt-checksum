package dbExec

import (
	"database/sql"
	mysql "greatdbCheck/MySQL"
	oracle "greatdbCheck/Oracle"
)

type DataAbnormalFixStruct struct {
}
type DataAbnormalFixInterface interface {
	FixInsertSqlExec(db *sql.DB, sourceDrive string) (string, error)
	FixDeleteSqlExec(db *sql.DB, sourceDrive string) (string, error)
}

func (dafs DataAbnormalFixStruct) DataAbnormalFix(dname, tname, rowdata string, coldata []map[string]string, sqlwhere, dbDevice, indexCt string) DataAbnormalFixInterface {
	var tqaci DataAbnormalFixInterface
	if dbDevice == "mysql" {
		tqaci = &mysql.MysqlDataAbnormalFixStruct{
			Schema:          dname,
			Table:           tname,
			Sqlwhere:        sqlwhere,
			RowData:         rowdata,
			SourceDevice:    dbDevice,
			IndexColumnType: indexCt,
			ColData:         coldata,
		}
	}
	if dbDevice == "godror" {
		tqaci = &oracle.OracleDataAbnormalFixStruct{
			Schema:          dname,
			Table:           tname,
			Sqlwhere:        sqlwhere,
			RowData:         rowdata,
			SourceDevice:    dbDevice,
			IndexColumnType: indexCt,
			ColData:         coldata,
		}
	}
	return tqaci
}
func DataFix() *DataAbnormalFixStruct {
	return &DataAbnormalFixStruct{}
}
