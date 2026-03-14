package actions

import (
	"testing"

	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
)

func ensureTestLogger(t *testing.T) {
	t.Helper()
	if global.Wlog != nil {
		return
	}
	handler, err := golog.NewNullHandler()
	if err != nil {
		t.Fatalf("NewNullHandler() error = %v", err)
	}
	global.Wlog = golog.NewDefault(handler)
}

func TestNormalizeStoredProcBodyStripsMetadataAndComments(t *testing.T) {
	ensureTestLogger(t)

	input := `
/*GT_CHECKSUM_METADATA:{"sql_mode":"STRICT_TRANS_TABLES"}*/
BEGIN
    -- inline comment
    SET s = n1 + n2; /* block comment */
END
`

	got := normalizeStoredProcBody(input)
	want := "BEGIN SET s = n1 + n2; END"
	if got != want {
		t.Fatalf("normalizeStoredProcBody() = %q, want %q", got, want)
	}
}

func TestExtractMetadataFromProcedure(t *testing.T) {
	ensureTestLogger(t)

	input := `
CREATE DEFINER='checksum'@'%' PROCEDURE myadd(IN n1 INT, IN n2 INT, OUT s INT)
SQL SECURITY DEFINER
/*GT_CHECKSUM_METADATA:{"character_set_client":"utf8mb4","database_collation":"utf8mb4_general_ci"}*/
BEGIN
    SET s = n1 + n2;
END
`

	got := extractMetadataFromProcedure(input)

	if got["DEFINER"] != "checksum@%" {
		t.Fatalf("unexpected definer: %q", got["DEFINER"])
	}
	if got["SQL_MODE"] != "DEFINER" {
		t.Fatalf("unexpected sql security metadata: %q", got["SQL_MODE"])
	}
	if got["CHARACTER_SET_CLIENT"] != "utf8mb4" {
		t.Fatalf("unexpected character_set_client: %q", got["CHARACTER_SET_CLIENT"])
	}
	if got["DATABASE_COLLATION"] != "utf8mb4_general_ci" {
		t.Fatalf("unexpected database_collation: %q", got["DATABASE_COLLATION"])
	}
}
