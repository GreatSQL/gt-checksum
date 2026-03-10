package inputArg

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"strings"
)

func queryMySQLVersion(db *sql.DB) (string, error) {
	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		return "", err
	}
	return version, nil
}

func (rc *ConfigParameter) validateMySQLCompatibility(srcDB, dstDB *sql.DB) error {
	srcVersionRaw, err := queryMySQLVersion(srcDB)
	if err != nil {
		return fmt.Errorf("query source MySQL version failed: %w", err)
	}
	dstVersionRaw, err := queryMySQLVersion(dstDB)
	if err != nil {
		return fmt.Errorf("query target MySQL version failed: %w", err)
	}

	srcVersion, err := global.ParseMySQLVersion(srcVersionRaw)
	if err != nil {
		return fmt.Errorf("parse source MySQL version failed: %w", err)
	}
	dstVersion, err := global.ParseMySQLVersion(dstVersionRaw)
	if err != nil {
		return fmt.Errorf("parse target MySQL version failed: %w", err)
	}

	global.SourceMySQLVersion = srcVersion
	global.DestMySQLVersion = dstVersion

	global.Wlog.Infof("(%d) [C_check_Options] source database detected: %s (series %s)", rc.LogThreadSeq, global.FormatDatabaseVersion(srcVersion), srcVersion.Series)
	global.Wlog.Infof("(%d) [C_check_Options] target database detected: %s (series %s)", rc.LogThreadSeq, global.FormatDatabaseVersion(dstVersion), dstVersion.Series)

	if err := global.ValidateMySQLCompatibilityPolicy(srcVersion, dstVersion, rc.SecondaryL.RulesV.CheckObject); err != nil {
		return err
	}

	if strings.EqualFold(rc.SecondaryL.RulesV.CheckObject, "data") {
		srcCharset := global.ExtractCharsetFromDSN(rc.SecondaryL.DsnsV.SrcJdbc)
		dstCharset := global.ExtractCharsetFromDSN(rc.SecondaryL.DsnsV.DestJdbc)
		if err := global.ValidateDataCheckCharset(srcCharset, dstCharset); err != nil {
			return err
		}
		global.Wlog.Infof("(%d) [C_check_Options] data check charset compatibility verified: source=%s target=%s", rc.LogThreadSeq, srcCharset, dstCharset)
	}

	return nil
}
