package schemacompat

import (
	"testing"
)

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
