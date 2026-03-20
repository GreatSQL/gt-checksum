package schemacompat

import "gt-checksum/global"

type SchemaFeatureCatalog struct {
	Flavor                     global.DatabaseFlavor
	Series                     string
	SupportsJSON               bool
	SupportsGeneratedColumns   bool
	SupportsInvisibleColumns   bool
	SupportsInvisibleIndexes   bool
	SupportsFunctionIndexes    bool
	SupportsCheckConstraints   bool
	EnforcesCheckConstraints   bool
	SupportsNativeUUIDType     bool
	SupportsNativeINET6Type    bool
	SupportsColumnCompression  bool
	StrictForeignKeyValidation bool
	DefaultCharset             string
	DefaultCollationByCharset  map[string]string
}

type DDLExecutionPolicy struct {
	Algorithm      string
	Lock           string
	AllowInstant   bool
	AllowLockNone  bool
	SupportsOnline bool
}

func DefaultDDLExecutionPolicy() DDLExecutionPolicy {
	return DDLExecutionPolicy{
		Algorithm:      "DEFAULT",
		Lock:           "",
		AllowInstant:   false,
		AllowLockNone:  false,
		SupportsOnline: false,
	}
}

func BuildSchemaFeatureCatalog(info global.MySQLVersionInfo) SchemaFeatureCatalog {
	catalog := SchemaFeatureCatalog{
		Flavor:         info.Flavor,
		Series:         info.Series,
		DefaultCharset: "utf8mb4",
		DefaultCollationByCharset: map[string]string{
			"utf8mb3":   "utf8mb3_general_ci",
			"utf8mb4":   "utf8mb4_general_ci",
			"latin1":    "latin1_swedish_ci",
			"binary":    "binary",
			"ascii":     "ascii_general_ci",
			"utf16":     "utf16_general_ci",
			"utf32":     "utf32_general_ci",
			"ucs2":      "ucs2_general_ci",
			"varbinary": "binary",
		},
	}

	switch info.Flavor {
	case global.DatabaseFlavorMariaDB:
		catalog.SupportsJSON = true
		catalog.SupportsGeneratedColumns = true
		catalog.SupportsInvisibleColumns = true
		// MariaDB surfaces optimizer-hidden index semantics via IGNORED indexes.
		// Track that capability from 10.6 onward so future feature gating does
		// not contradict the existing IGNORE -> INVISIBLE rewrite path.
		catalog.SupportsInvisibleIndexes = info.Major > 10 || (info.Major == 10 && info.Minor >= 6)
		catalog.SupportsFunctionIndexes = true
		catalog.SupportsCheckConstraints = true
		catalog.EnforcesCheckConstraints = true
		catalog.SupportsNativeINET6Type = info.Major > 10 || (info.Major == 10 && info.Minor >= 5)
		catalog.SupportsNativeUUIDType = info.Major > 10 || (info.Major == 10 && info.Minor >= 7)
		catalog.SupportsColumnCompression = true
		// MariaDB 11.5+ uses utf8mb4_uca1400_ai_ci as the default collation for utf8mb4.
		if info.Major > 11 || (info.Major == 11 && info.Minor >= 5) {
			catalog.DefaultCollationByCharset["utf8mb4"] = "utf8mb4_uca1400_ai_ci"
		} else {
			catalog.DefaultCollationByCharset["utf8mb4"] = "utf8mb4_general_ci"
		}
	default:
		catalog.SupportsJSON = info.Major > 5 || (info.Major == 5 && info.Minor >= 7)
		catalog.SupportsGeneratedColumns = info.Major > 5 || (info.Major == 5 && info.Minor >= 7)
		catalog.SupportsInvisibleColumns = info.Major > 8 || (info.Major == 8 && info.Minor >= 0)
		catalog.SupportsInvisibleIndexes = info.Major > 8 || (info.Major == 8 && info.Minor >= 0)
		catalog.SupportsFunctionIndexes = info.Major > 8 || (info.Major == 8 && info.Minor >= 0)
		catalog.SupportsCheckConstraints = info.Major > 8 || (info.Major == 8 && info.Minor >= 0)
		catalog.EnforcesCheckConstraints = info.Major > 8 ||
			(info.Major == 8 && info.Minor > 0) ||
			(info.Major == 8 && info.Minor == 0 && info.Patch >= 16)
		catalog.StrictForeignKeyValidation = info.Series == "8.4"
		if info.Major > 8 || (info.Major == 8 && info.Minor >= 0) {
			catalog.DefaultCollationByCharset["utf8mb4"] = "utf8mb4_0900_ai_ci"
		}
	}

	return catalog
}
