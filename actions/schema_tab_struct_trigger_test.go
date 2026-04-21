package actions

import (
	"testing"

	"gt-checksum/global"
)

// ---------- shouldCompareTriggerMetadata ----------

func TestShouldCompareTriggerMetadata(t *testing.T) {
	tests := []struct {
		name     string
		st       *schemaTable
		expected bool
	}{
		{
			name:     "mariadb-to-mariadb-returns-true",
			st:       makeSchemaTable("10.6.12-MariaDB", "10.6", global.DatabaseFlavorMariaDB, "10.11.5-MariaDB", "10.11", global.DatabaseFlavorMariaDB),
			expected: true,
		},
		{
			name:     "mysql-to-mariadb-returns-false",
			st:       makeSchemaTable("8.0.33", "8.0", global.DatabaseFlavorMySQL, "10.11.5-MariaDB", "10.11", global.DatabaseFlavorMariaDB),
			expected: false,
		},
		{
			name:     "mysql-to-mysql-returns-true",
			st:       makeSchemaTable("8.0.33", "8.0", global.DatabaseFlavorMySQL, "8.0.35", "8.0", global.DatabaseFlavorMySQL),
			expected: true,
		},
		{
			name:     "mariadb-to-mysql80-returns-true",
			st:       makeSchemaTable("10.6.12-MariaDB", "10.6", global.DatabaseFlavorMariaDB, "8.0.33", "8.0", global.DatabaseFlavorMySQL),
			expected: true,
		},
		{
			name:     "mariadb-to-mysql57-returns-false",
			st:       makeSchemaTable("10.6.12-MariaDB", "10.6", global.DatabaseFlavorMariaDB, "5.7.42", "5.7", global.DatabaseFlavorMySQL),
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.st.shouldCompareTriggerMetadata(); got != tt.expected {
				t.Fatalf("shouldCompareTriggerMetadata() = %v, want %v", got, tt.expected)
			}
		})
	}
}
