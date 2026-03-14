//go:build cgo

package dbExec

import oracle "gt-checksum/Oracle"

func newOracleTableIndexColumner(qticis *IndexColumnStruct) TableIndexColumner {
	return &oracle.QueryTable{
		Schema:       qticis.Schema,
		Table:        qticis.Table,
		ColumnName:   qticis.ColumnName,
		ChanrowCount: qticis.ChanrowCount,
		TableColumn:  qticis.TableColumn,
		Sqlwhere:     qticis.Sqlwhere,
		ColData:      qticis.ColData,
		SelectColumn: qticis.SelectColumn,
		BeginSeq:     qticis.BeginSeq,
		RowDataCh:    qticis.RowDataCh,
	}
}
