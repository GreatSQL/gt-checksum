package global

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var mysqlVersionRegexp = regexp.MustCompile(`(\d+)\.(\d+)(?:\.(\d+))?`)

var supportedMySQLSeries = map[string]struct{}{
	"5.6": {},
	"5.7": {},
	"8.0": {},
	"8.4": {},
}

type DatabaseFlavor string

const (
	DatabaseFlavorMySQL   DatabaseFlavor = "MySQL"
	DatabaseFlavorMariaDB DatabaseFlavor = "MariaDB"
)

type MySQLVersionInfo struct {
	Raw    string
	Series string
	Major  int
	Minor  int
	Patch  int
	Flavor DatabaseFlavor
}

var (
	SourceMySQLVersion MySQLVersionInfo
	DestMySQLVersion   MySQLVersionInfo
)

func ParseMySQLVersion(raw string) (MySQLVersionInfo, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return MySQLVersionInfo{}, fmt.Errorf("empty MySQL version string")
	}

	matches := mysqlVersionRegexp.FindStringSubmatch(trimmed)
	if len(matches) < 3 {
		return MySQLVersionInfo{}, fmt.Errorf("cannot parse MySQL version from %q", raw)
	}

	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return MySQLVersionInfo{}, fmt.Errorf("parse MySQL major version %q: %w", matches[1], err)
	}
	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		return MySQLVersionInfo{}, fmt.Errorf("parse MySQL minor version %q: %w", matches[2], err)
	}

	patch := 0
	if len(matches) > 3 && matches[3] != "" {
		patch, err = strconv.Atoi(matches[3])
		if err != nil {
			return MySQLVersionInfo{}, fmt.Errorf("parse MySQL patch version %q: %w", matches[3], err)
		}
	}

	return MySQLVersionInfo{
		Raw:    trimmed,
		Series: fmt.Sprintf("%d.%d", major, minor),
		Major:  major,
		Minor:  minor,
		Patch:  patch,
		Flavor: DetectDatabaseFlavor(trimmed),
	}, nil
}

func DetectDatabaseFlavor(raw string) DatabaseFlavor {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if strings.Contains(normalized, "mariadb") {
		return DatabaseFlavorMariaDB
	}
	return DatabaseFlavorMySQL
}

func (info MySQLVersionInfo) FlavorName() string {
	if info.Flavor == "" {
		return string(DatabaseFlavorMySQL)
	}
	return string(info.Flavor)
}

func FormatDatabaseVersion(info MySQLVersionInfo) string {
	return fmt.Sprintf("%s %s", info.FlavorName(), info.Raw)
}

func SupportedMySQLSeriesList() string {
	return "5.6, 5.7, 8.0, 8.4"
}

func IsSupportedMySQLSeries(series string) bool {
	_, ok := supportedMySQLSeries[series]
	return ok
}

func CompareMySQLVersionSeries(src, dst MySQLVersionInfo) int {
	if src.Major != dst.Major {
		if src.Major < dst.Major {
			return -1
		}
		return 1
	}
	if src.Minor != dst.Minor {
		if src.Minor < dst.Minor {
			return -1
		}
		return 1
	}
	return 0
}

func ValidateMySQLVersionPair(src, dst MySQLVersionInfo) error {
	if !IsSupportedMySQLSeries(src.Series) {
		return fmt.Errorf("unsupported source MySQL version %s (series %s); supported series are: %s", src.Raw, src.Series, SupportedMySQLSeriesList())
	}
	if !IsSupportedMySQLSeries(dst.Series) {
		return fmt.Errorf("unsupported target MySQL version %s (series %s); supported series are: %s", dst.Raw, dst.Series, SupportedMySQLSeriesList())
	}
	if CompareMySQLVersionSeries(src, dst) > 0 {
		return fmt.Errorf("source MySQL version %s (series %s) is higher than target %s (series %s); downgrade check/repair is not supported", src.Raw, src.Series, dst.Raw, dst.Series)
	}
	return nil
}

func ValidateMySQLCompatibilityPolicy(src, dst MySQLVersionInfo, checkObject string) error {
	normalizedCheckObject := strings.ToLower(strings.TrimSpace(checkObject))

	switch {
	case src.Flavor == DatabaseFlavorMariaDB && dst.Flavor == DatabaseFlavorMySQL:
		if dst.Series != "8.0" && dst.Series != "8.4" {
			return fmt.Errorf("source database %s only supports data check/fix to MySQL 8.0 or 8.4 targets; current destination %s is not supported", FormatDatabaseVersion(src), FormatDatabaseVersion(dst))
		}
		switch normalizedCheckObject {
		case "data", "struct", "routine", "trigger":
			return nil
		default:
			return fmt.Errorf("source database %s to destination %s only supports checkObject=data, struct, routine or trigger; checkObject=%s is not supported", FormatDatabaseVersion(src), FormatDatabaseVersion(dst), checkObject)
		}
	case src.Flavor == DatabaseFlavorMySQL && dst.Flavor == DatabaseFlavorMariaDB:
		return fmt.Errorf("source database %s to destination %s is not supported", FormatDatabaseVersion(src), FormatDatabaseVersion(dst))
	case src.Flavor == DatabaseFlavorMariaDB && dst.Flavor == DatabaseFlavorMariaDB:
		return fmt.Errorf("source database %s to destination %s is not supported; only MariaDB -> MySQL 8.0/8.4 data/struct/routine/trigger check and fix are supported", FormatDatabaseVersion(src), FormatDatabaseVersion(dst))
	default:
		return ValidateMySQLVersionPair(src, dst)
	}
}

func ValidateDataCheckCharset(srcCharset, dstCharset string) error {
	normalizedSrc := strings.TrimSpace(srcCharset)
	if normalizedSrc == "" {
		normalizedSrc = "utf8mb4"
	}

	normalizedDst := strings.TrimSpace(dstCharset)
	if normalizedDst == "" {
		normalizedDst = "utf8mb4"
	}

	if !strings.EqualFold(normalizedSrc, normalizedDst) {
		return fmt.Errorf("data check/fix requires identical DSN charsets; source uses %s and target uses %s. Please align srcDSN and dstDSN charset settings before retrying", normalizedSrc, normalizedDst)
	}
	return nil
}

func BuildMySQLSessionPreamble(charset string) []string {
	normalizedCharset := strings.TrimSpace(charset)
	if normalizedCharset == "" {
		normalizedCharset = "utf8mb4"
	}

	return []string{
		fmt.Sprintf("SET NAMES %s;", normalizedCharset),
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET UNIQUE_CHECKS=0;",
		"SET INNODB_LOCK_WAIT_TIMEOUT=1073741824;",
		"/*!80013 SET SESSION sql_require_primary_key=0 */;",
		"/*!80030 SET SESSION sql_generate_invisible_primary_key=0 */;",
	}
}
