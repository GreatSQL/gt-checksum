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

type MySQLVersionInfo struct {
	Raw    string
	Series string
	Major  int
	Minor  int
	Patch  int
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
	}, nil
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
