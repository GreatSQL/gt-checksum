//go:build !cgo

package dbExec

func newOracleTableColumnQuery(tcns *TableColumnNameStruct) QueryTableColumnNameInterface {
	// Oracle metadata path stays behind cgo because the structural regression
	// baseline only validates MySQL/MariaDB code paths.
	return nil
}
