//go:build cgo

package dbExec

import oracle "gt-checksum/Oracle"

func newOracleTableColumnQuery(tcns *TableColumnNameStruct) QueryTableColumnNameInterface {
	return &oracle.QueryTable{
		Schema:                  tcns.Schema,
		Table:                   tcns.Table,
		Db:                      tcns.Db,
		CaseSensitiveObjectName: "",
	}
}
