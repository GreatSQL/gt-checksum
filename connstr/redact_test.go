package connstr

import (
	"strings"
	"testing"
)

func assertNoSecret(t *testing.T, redacted string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(redacted, secret) {
			t.Fatalf("redacted DSN %q still contains secret %q", redacted, secret)
		}
	}
}

func TestRedactMySQLDSN(t *testing.T) {
	redacted := RedactDSN("mysql", "user:plain-pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4")
	want := "user:******@tcp(127.0.0.1:3306)/db?charset=utf8mb4"
	if redacted != want {
		t.Fatalf("RedactDSN() = %q, want %q", redacted, want)
	}
	assertNoSecret(t, redacted, "plain-pass")
}

func TestRedactMySQLEncryptedDSN(t *testing.T) {
	ciphertext := encryptedPassword(t, "plain-pass")
	redacted := RedactDSN("mysql", "user:"+ciphertext+"@tcp(127.0.0.1:3306)/db")
	want := "user:******@tcp(127.0.0.1:3306)/db"
	if redacted != want {
		t.Fatalf("RedactDSN() = %q, want %q", redacted, want)
	}
	assertNoSecret(t, redacted, ciphertext)
}

func TestRedactFullDSN(t *testing.T) {
	redacted := RedactFullDSN("mysql|user:plain-pass@tcp(127.0.0.1:3306)/db")
	want := "mysql|user:******@tcp(127.0.0.1:3306)/db"
	if redacted != want {
		t.Fatalf("RedactFullDSN() = %q, want %q", redacted, want)
	}
	assertNoSecret(t, redacted, "plain-pass")
}

func TestRedactDefaultFullDSN(t *testing.T) {
	redacted := RedactFullDSN("user:plain-pass@tcp(127.0.0.1:3306)/db")
	want := "user:******@tcp(127.0.0.1:3306)/db"
	if redacted != want {
		t.Fatalf("RedactFullDSN() = %q, want %q", redacted, want)
	}
}

func TestRedactOracleLegacyDSN(t *testing.T) {
	redacted := RedactDSN("oracle", "scott/tiger@127.0.0.1:1521/orclpdb1?timezone=UTC")
	want := "scott/******@127.0.0.1:1521/orclpdb1?timezone=UTC"
	if redacted != want {
		t.Fatalf("RedactDSN() = %q, want %q", redacted, want)
	}
	assertNoSecret(t, redacted, "tiger")
}

func TestRedactOracleKeyValueDSN(t *testing.T) {
	redacted := RedactDSN("oracle", `connectString="127.0.0.1:1521/orclpdb1" user="scott" password="tiger" timezone="UTC"`)
	want := `connectString="127.0.0.1:1521/orclpdb1" user="scott" password="******" timezone="UTC"`
	if redacted != want {
		t.Fatalf("RedactDSN() = %q, want %q", redacted, want)
	}
	assertNoSecret(t, redacted, "tiger")
}

func TestRedactOracleKeyValueUnquotedPassword(t *testing.T) {
	redacted := RedactDSN("oracle", `user=scott password=tiger connectString="127.0.0.1:1521/orclpdb1"`)
	want := `user=scott password="******" connectString="127.0.0.1:1521/orclpdb1"`
	if redacted != want {
		t.Fatalf("RedactDSN() = %q, want %q", redacted, want)
	}
	assertNoSecret(t, redacted, "tiger")
}

func TestRedactOracleKeyValuePasswordWithSpacesAroundEquals(t *testing.T) {
	redacted := RedactDSN("oracle", `user = "scott" password = "tiger" connectString = "127.0.0.1:1521/orclpdb1"`)
	want := `user = "scott" password = "******" connectString = "127.0.0.1:1521/orclpdb1"`
	if redacted != want {
		t.Fatalf("RedactDSN() = %q, want %q", redacted, want)
	}
	assertNoSecret(t, redacted, "tiger")
}

func TestRedactOracleMalformedKeyValueDSN(t *testing.T) {
	redacted := RedactDSN("oracle", `user="scott" password="tiger`)
	assertNoSecret(t, redacted, "tiger")
	if !strings.Contains(redacted, "******") {
		t.Fatalf("redacted DSN should contain mask: %q", redacted)
	}
}

func TestRedactUnknownDriverPasswordAssignment(t *testing.T) {
	redacted := RedactDSN("unknown", `user=scott password = tiger host=127.0.0.1`)
	assertNoSecret(t, redacted, "tiger")
	if !strings.Contains(redacted, `password = "******"`) {
		t.Fatalf("redacted DSN should mask password assignment: %q", redacted)
	}
}
