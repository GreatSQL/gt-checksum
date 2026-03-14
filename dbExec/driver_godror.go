//go:build cgo

package dbExec

import (
	// Keep Oracle driver registration behind cgo so MySQL/MariaDB-focused
	// unit tests can run with CGO_ENABLED=0 without pulling godror into the
	// default test compilation path.
	_ "github.com/godror/godror"
)
