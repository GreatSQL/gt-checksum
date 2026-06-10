package inputArg

import "testing"

func TestNormalizeOracleJDBCKeyValueWithSpacesAroundEquals(t *testing.T) {
	jdbc := `user = "scott" password = "tiger" connectString = "127.0.0.1:1521/orclpdb1"`
	got, err := normalizeOracleJDBC(jdbc)
	if err != nil {
		t.Fatalf("normalizeOracleJDBC() error = %v", err)
	}
	if got != jdbc {
		t.Fatalf("normalizeOracleJDBC() = %q, want unchanged %q", got, jdbc)
	}
}
