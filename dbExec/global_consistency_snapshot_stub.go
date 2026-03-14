//go:build !cgo

package dbExec

func newOracleGlobalCS(poolMin int, jdbc, dbDevice string) DBGlobalCS {
	// Oracle global-consistency snapshot is not part of the MySQL/MariaDB
	// regression baseline that runs with CGO disabled.
	return nil
}
