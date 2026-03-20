package schemacompat

import (
	"regexp"
	"strconv"
	"strings"
)

type ColumnShrinkMetric string

const (
	ColumnShrinkMetricCharLength  ColumnShrinkMetric = "CHAR_LENGTH"
	ColumnShrinkMetricOctetLength ColumnShrinkMetric = "OCTET_LENGTH"
)

// IsValid returns true only for the two known safe MySQL measurement functions.
func (m ColumnShrinkMetric) IsValid() bool {
	return m == ColumnShrinkMetricCharLength || m == ColumnShrinkMetricOctetLength
}

type ColumnShrinkGuard struct {
	Metric     ColumnShrinkMetric
	Limit      int
	SourceType string
	TargetType string
}

var widthLimitedColumnTypeRegexp = regexp.MustCompile(`^(char|varchar|binary|varbinary)\((\d+)\)$`)

func BuildColumnShrinkGuard(source, target CanonicalColumn) (ColumnShrinkGuard, bool) {
	sourceGroup, sourceMetric, sourceLimit, ok := parseWidthLimitedColumnType(source.NormalizedType)
	if !ok {
		return ColumnShrinkGuard{}, false
	}

	targetGroup, _, targetLimit, ok := parseWidthLimitedColumnType(target.NormalizedType)
	if !ok {
		return ColumnShrinkGuard{}, false
	}

	if sourceGroup != targetGroup {
		return ColumnShrinkGuard{}, false
	}
	if sourceLimit >= targetLimit {
		return ColumnShrinkGuard{}, false
	}

	return ColumnShrinkGuard{
		Metric:     sourceMetric,
		Limit:      sourceLimit,
		SourceType: normalizeWhitespace(strings.ToLower(source.NormalizedType)),
		TargetType: normalizeWhitespace(strings.ToLower(target.NormalizedType)),
	}, true
}

func parseWidthLimitedColumnType(normalizedType string) (string, ColumnShrinkMetric, int, bool) {
	matches := widthLimitedColumnTypeRegexp.FindStringSubmatch(normalizeWhitespace(strings.ToLower(normalizedType)))
	if len(matches) != 3 {
		return "", "", 0, false
	}

	limit, err := strconv.Atoi(matches[2])
	if err != nil {
		return "", "", 0, false
	}

	switch matches[1] {
	case "char", "varchar":
		return "character", ColumnShrinkMetricCharLength, limit, true
	case "binary", "varbinary":
		return "binary", ColumnShrinkMetricOctetLength, limit, true
	default:
		return "", "", 0, false
	}
}
