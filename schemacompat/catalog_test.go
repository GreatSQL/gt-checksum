package schemacompat

import (
	"testing"

	"gt-checksum/global"
)

func TestBuildTargetColumnRepairPlanMariaDBToMariaDB(t *testing.T) {
	mariaDB106 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMariaDB, Major: 10, Minor: 6, Series: "10.6"}
	mariaDB1011 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMariaDB, Major: 10, Minor: 11, Series: "10.11"}
	mysql80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0, Series: "8.0"}

	tests := []struct {
		name        string
		columnType  string
		srcInfo     global.MySQLVersionInfo
		dstInfo     global.MySQLVersionInfo
		wantContain string // 期望输出中包含该字符串（空表示不检查）
		wantStrip   string // 期望输出中不含该字符串（空表示不检查）
	}{
		{
			name:        "mariadb-to-mariadb-preserves-COMPRESSED",
			columnType:  "varchar(100) COMPRESSED",
			srcInfo:     mariaDB106,
			dstInfo:     mariaDB1011,
			wantContain: "COMPRESSED",
		},
		{
			name:        "mariadb-to-mariadb-preserves-PERSISTENT",
			columnType:  "int PERSISTENT",
			srcInfo:     mariaDB106,
			dstInfo:     mariaDB106,
			wantContain: "PERSISTENT",
		},
		{
			name:      "mariadb-to-mysql-strips-COMPRESSED",
			columnType: "varchar(100) COMPRESSED",
			srcInfo:    mariaDB106,
			dstInfo:    mysql80,
			wantStrip:  "COMPRESSED",
		},
		{
			name:      "mariadb-to-mysql-strips-PERSISTENT",
			columnType: "int PERSISTENT",
			srcInfo:    mariaDB106,
			dstInfo:    mysql80,
			wantStrip:  "PERSISTENT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := BuildTargetColumnRepairPlan("col", []string{tt.columnType, "", ""}, tt.srcInfo, tt.dstInfo, "", "")
			if tt.wantContain != "" && !containsStr(plan.Type, tt.wantContain) {
				t.Fatalf("expected plan.Type %q to contain %q", plan.Type, tt.wantContain)
			}
			if tt.wantStrip != "" && containsStr(plan.Type, tt.wantStrip) {
				t.Fatalf("expected plan.Type %q NOT to contain %q (should have been stripped)", plan.Type, tt.wantStrip)
			}
		})
	}
}

func containsStr(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && func() bool {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()
}

func TestBuildSchemaFeatureCatalogMariaDBInvisibleIndexes(t *testing.T) {
	tests := []struct {
		name     string
		info     global.MySQLVersionInfo
		expected bool
	}{
		{
			name: "mariadb-10.5-does-not-advertise-ignored-index-capability",
			info: global.MySQLVersionInfo{
				Flavor: global.DatabaseFlavorMariaDB,
				Major:  10,
				Minor:  5,
			},
			expected: false,
		},
		{
			name: "mariadb-10.11-advertises-ignored-index-capability",
			info: global.MySQLVersionInfo{
				Flavor: global.DatabaseFlavorMariaDB,
				Major:  10,
				Minor:  11,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := BuildSchemaFeatureCatalog(tt.info)
			if catalog.SupportsInvisibleIndexes != tt.expected {
				t.Fatalf("SupportsInvisibleIndexes = %v, want %v", catalog.SupportsInvisibleIndexes, tt.expected)
			}
		})
	}
}
