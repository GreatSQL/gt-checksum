package schemacompat

import (
	"gt-checksum/global"
	"testing"
)

func TestZeroFillAttributeDetection(t *testing.T) {
	mysql56 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 5, Minor: 6, Series: "5.6"}
	mysql80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0, Series: "8.0"}

	tests := []struct {
		name           string
		sourceType     string
		targetType     string
		srcVersion     global.MySQLVersionInfo
		dstVersion     global.MySQLVersionInfo
		expectMismatch bool
		expectReason   string
	}{
		{
			name:           "zerofill-same-column-match",
			sourceType:     "int(5) unsigned zerofill",
			targetType:     "int(5) unsigned zerofill",
			srcVersion:     mysql56,
			dstVersion:     mysql80,
			expectMismatch: false,
		},
		{
			name:           "zerofill-different-column-mismatch",
			sourceType:     "int(5) unsigned zerofill",
			targetType:     "int unsigned",
			srcVersion:     mysql56,
			dstVersion:     mysql80,
			expectMismatch: true,
			expectReason:   "zerofill attribute differs",
		},
		{
			name:           "zerofill-swapped-columns-mismatch",
			sourceType:     "int(10) unsigned",
			targetType:     "int(3) unsigned zerofill",
			srcVersion:     mysql56,
			dstVersion:     mysql80,
			expectMismatch: true,
			expectReason:   "zerofill attribute differs",
		},
		{
			name:           "no-zerofill-both-match",
			sourceType:     "int unsigned",
			targetType:     "int unsigned",
			srcVersion:     mysql56,
			dstVersion:     mysql80,
			expectMismatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceCol := CanonicalizeMySQLColumn("test_col", []string{tt.sourceType, "", "", "YES", "", ""}, tt.srcVersion)
			targetCol := CanonicalizeMySQLColumn("test_col", []string{tt.targetType, "", "", "YES", "", ""}, tt.dstVersion)

			decision := DecideColumnDefinitionCompatibility(sourceCol, targetCol)

			if tt.expectMismatch {
				if !decision.IsMismatch() {
					t.Errorf("Expected mismatch but got match. Source: %s, Target: %s", tt.sourceType, tt.targetType)
				}
				if tt.expectReason != "" && decision.Reason != "" {
					if !contains(decision.Reason, tt.expectReason) {
						t.Errorf("Expected reason to contain '%s', got '%s'", tt.expectReason, decision.Reason)
					}
				}
			} else {
				if decision.IsMismatch() {
					t.Errorf("Expected match but got mismatch. Source: %s, Target: %s, Reason: %s", tt.sourceType, tt.targetType, decision.Reason)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

