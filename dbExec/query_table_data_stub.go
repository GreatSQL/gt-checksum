//go:build !cgo

package dbExec

func newOracleTableIndexColumner(qticis *IndexColumnStruct) TableIndexColumner {
	// Oracle data path is intentionally unavailable in the CGO-disabled test
	// baseline. Returning nil keeps MySQL/MariaDB-only packages compilable
	// without silently changing the production Oracle implementation.
	return nil
}
