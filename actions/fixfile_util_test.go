package actions

import (
	"testing"
)

func TestFixFileNameEncode(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"plain ASCII", "orders", "orders"},
		{"with underscore and hyphen", "my_table-1", "my_table-1"},
		{"mixed case", "MySchema", "MySchema"},
		{"dot in name", "db.sub", "db%2Esub"},
		{"space", "my schema", "my%20schema"},
		{"backtick", "my`table", "my%60table"},
		{"slash", "a/b", "a%2Fb"},
		{"backslash", `a\b`, "a%5Cb"},
		{"chinese", "订单", "%E8%AE%A2%E5%8D%95"},
		{"at sign", "user@host", "user%40host"},
		{"empty string", "", ""},
		{"numbers only", "123", "123"},
		{"all safe chars", "Az09_-", "Az09_-"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := fixFileNameEncode(tc.input)
			if got != tc.want {
				t.Errorf("fixFileNameEncode(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
