package schemacompat

import (
	"testing"

	"gt-checksum/global"
)

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
