package schemacompat

import (
	"testing"
)

// TestDecideCollationCompatibility_UTF8MB4DefaultDrift 测试 MySQL 5.6/5.7 到 8.0 的 utf8mb4 默认 collation 漂移
// 修复后应返回 CompatibilityUnsupported 而不是 CompatibilityWarnOnly
func TestDecideCollationCompatibility_UTF8MB4DefaultDrift(t *testing.T) {
	tests := []struct {
		name          string
		source        string
		target        string
		expectedState CompatibilityState
	}{
		{
			name:          "MySQL 5.6/5.7 to 8.0: utf8mb4_general_ci -> utf8mb4_0900_ai_ci",
			source:        "utf8mb4_general_ci",
			target:        "utf8mb4_0900_ai_ci",
			expectedState: CompatibilityUnsupported,
		},
		{
			name:          "MySQL 8.0 to 5.6/5.7: utf8mb4_0900_ai_ci -> utf8mb4_general_ci",
			source:        "utf8mb4_0900_ai_ci",
			target:        "utf8mb4_general_ci",
			expectedState: CompatibilityUnsupported,
		},
		{
			name:          "Same collation: utf8mb4_general_ci -> utf8mb4_general_ci",
			source:        "utf8mb4_general_ci",
			target:        "utf8mb4_general_ci",
			expectedState: CompatibilityEqual,
		},
		{
			name:          "Same collation: utf8mb4_0900_ai_ci -> utf8mb4_0900_ai_ci",
			source:        "utf8mb4_0900_ai_ci",
			target:        "utf8mb4_0900_ai_ci",
			expectedState: CompatibilityEqual,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := DecideCollationCompatibility(tt.source, tt.target)
			if decision.State != tt.expectedState {
				t.Errorf("DecideCollationCompatibility(%q, %q) state = %v, want %v (reason: %s)",
					tt.source, tt.target, decision.State, tt.expectedState, decision.Reason)
			}
		})
	}
}

// TestDecideCollationCompatibility_MariaDBToMySQL 测试 MariaDB→MySQL 跨平台 collation 映射场景
// 修复后应返回 CompatibilityWarnOnly，标记为 collation-mapped，不生成修复 SQL
func TestDecideCollationCompatibility_MariaDBToMySQL(t *testing.T) {
	tests := []struct {
		name          string
		source        string
		target        string
		expectedState CompatibilityState
	}{
		{
			name:          "MariaDB 12.3 to MySQL 8.0: utf8mb4_uca1400_ai_ci -> utf8mb4_0900_ai_ci",
			source:        "utf8mb4_uca1400_ai_ci",
			target:        "utf8mb4_0900_ai_ci",
			expectedState: CompatibilityWarnOnly,
		},
		{
			name:          "MySQL 8.0 to MariaDB 12.3: utf8mb4_0900_ai_ci -> utf8mb4_uca1400_ai_ci",
			source:        "utf8mb4_0900_ai_ci",
			target:        "utf8mb4_uca1400_ai_ci",
			expectedState: CompatibilityWarnOnly,
		},
		{
			name:          "MariaDB to MySQL with locale variant: utf8mb4_uca1400_swedish_ai_ci -> utf8mb4_0900_ai_ci",
			source:        "utf8mb4_uca1400_swedish_ai_ci",
			target:        "utf8mb4_0900_ai_ci",
			expectedState: CompatibilityWarnOnly,
		},
		{
			name:          "MariaDB to MySQL as_ci: utf8mb4_uca1400_as_ci -> utf8mb4_0900_as_ci",
			source:        "utf8mb4_uca1400_as_ci",
			target:        "utf8mb4_0900_as_ci",
			expectedState: CompatibilityWarnOnly,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := DecideCollationCompatibility(tt.source, tt.target)
			if decision.State != tt.expectedState {
				t.Errorf("DecideCollationCompatibility(%q, %q) state = %v, want %v (reason: %s)",
					tt.source, tt.target, decision.State, tt.expectedState, decision.Reason)
			}
		})
	}
}

// TestMapMariaDBCollationToMySQL 测试 MariaDB UCA 14.0.0 collation 到 MySQL UCA 9.0.0 的映射
func TestMapMariaDBCollationToMySQL(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedOutput string
		expectedIsUCA  bool
	}{
		{
			name:           "MariaDB 12.3 default utf8mb4_uca1400_ai_ci",
			input:          "utf8mb4_uca1400_ai_ci",
			expectedOutput: "utf8mb4_0900_ai_ci",
			expectedIsUCA:  true,
		},
		{
			name:           "MariaDB utf8mb4_uca1400_as_ci",
			input:          "utf8mb4_uca1400_as_ci",
			expectedOutput: "utf8mb4_0900_as_ci",
			expectedIsUCA:  true,
		},
		{
			name:           "MariaDB utf8mb4_uca1400_as_cs",
			input:          "utf8mb4_uca1400_as_cs",
			expectedOutput: "utf8mb4_0900_as_cs",
			expectedIsUCA:  true,
		},
		{
			name:           "MariaDB utf8mb4_uca1400_ai_cs",
			input:          "utf8mb4_uca1400_ai_cs",
			expectedOutput: "utf8mb4_0900_ai_cs",
			expectedIsUCA:  true,
		},
		{
			name:           "MariaDB locale variant utf8mb4_uca1400_swedish_ai_ci",
			input:          "utf8mb4_uca1400_swedish_ai_ci",
			expectedOutput: "utf8mb4_0900_ai_ci",
			expectedIsUCA:  true,
		},
		{
			name:           "Non-UCA1400 collation utf8mb4_general_ci",
			input:          "utf8mb4_general_ci",
			expectedOutput: "",
			expectedIsUCA:  false,
		},
		{
			name:           "MySQL 8.0 collation utf8mb4_0900_ai_ci",
			input:          "utf8mb4_0900_ai_ci",
			expectedOutput: "",
			expectedIsUCA:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, isUCA := MapMariaDBCollationToMySQL(tt.input)
			if output != tt.expectedOutput {
				t.Errorf("MapMariaDBCollationToMySQL(%q) output = %q, want %q", tt.input, output, tt.expectedOutput)
			}
			if isUCA != tt.expectedIsUCA {
				t.Errorf("MapMariaDBCollationToMySQL(%q) isUCA = %v, want %v", tt.input, isUCA, tt.expectedIsUCA)
			}
		})
	}
}
