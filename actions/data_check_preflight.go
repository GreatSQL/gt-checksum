package actions

type DataCheckPreflightDecision struct {
	SkipChecksum bool
	Fatal        bool
	Message      string
}

func EvaluateDataCheckPreflight(validCount, abnormalCount int, hasInvisibleMismatch bool) DataCheckPreflightDecision {
	if validCount > 0 {
		return DataCheckPreflightDecision{}
	}

	if abnormalCount > 0 {
		if hasInvisibleMismatch {
			return DataCheckPreflightDecision{
				SkipChecksum: true,
				Message:      "gt-checksum: All candidate tables have DDL differences (including invisible columns); skipping data validation and reporting DDL-yes status",
			}
		}
		return DataCheckPreflightDecision{
			SkipChecksum: true,
			Message:      "gt-checksum: All candidate tables have DDL differences; skipping data validation and reporting DDL-yes status",
		}
	}

	if hasInvisibleMismatch {
		return DataCheckPreflightDecision{
			Fatal:   true,
			Message: "gt-checksum: Source and target table structure mismatch (invisible columns detected), skipping data validation for these tables",
		}
	}

	return DataCheckPreflightDecision{
		Fatal:   true,
		Message: "gt-checksum: No valid tables in checklist. Check log file or set logLevel=debug for details",
	}
}
