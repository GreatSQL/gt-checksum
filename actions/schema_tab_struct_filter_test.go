package actions

import (
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gt-checksum/dbExec"
	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
)

func TestPrivPrecheck_SourceMetadataEmptySelectHintTargets(t *testing.T) {
	got := sourceMetadataPrivilegeHintTargets("gt_checksum.*,srcdb.*:dstdb.*,app.t1,app.t%,*.all")
	want := []string{"gt_checksum.*", "srcdb.*", "app.t1", "app.*"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("source metadata privilege hint targets = %v, want %v", got, want)
	}
}

func TestPrivPrecheck_SourceMetadataEmptySelectGrantHint(t *testing.T) {
	targets := sourceMetadataPrivilegeHintTargets("gt_checksum.*")
	grantHints := sourceMetadataPrivilegeGrantHints(targets)
	got := strings.Join(grantHints, " ")
	want := "GRANT SELECT ON `gt_checksum`.* TO '<source_user>'@'<host>';"
	if !strings.Contains(got, want) {
		t.Fatalf("grant hints = %q, want to contain %q", got, want)
	}
}

func TestPrivPrecheck_NoMatchedTablesLogsPrivilegeHint(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "priv-precheck.log")
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(logPath, "debug")
	defer func() { global.Wlog = origWlog }()

	origDBList := schemaTableFilterDatabaseNameList
	origObjectTypeMap := schemaTableFilterObjectTypeMap
	origAccessCheck := schemaTableFilterTableAccessPriCheck
	defer func() {
		schemaTableFilterDatabaseNameList = origDBList
		schemaTableFilterObjectTypeMap = origObjectTypeMap
		schemaTableFilterTableAccessPriCheck = origAccessCheck
	}()

	schemaTableFilterDatabaseNameList = func(tc dbExec.TableColumnNameStruct, db *sql.DB, logThreadSeq int64) (map[string]int, error) {
		return map[string]int{
			"mysql/*schema&table*/user": 1,
		}, nil
	}
	schemaTableFilterObjectTypeMap = func(tc dbExec.TableColumnNameStruct, db *sql.DB, logThreadSeq int64) (map[string]string, error) {
		return map[string]string{}, nil
	}

	var gotAccessTargets []string
	schemaTableFilterTableAccessPriCheck = func(tc dbExec.TableColumnNameStruct, db *sql.DB, checkTableList []string, checkObject, datafix, accessRole string, logThreadSeq int64) (map[string]int, error) {
		gotAccessTargets = append([]string(nil), checkTableList...)
		if accessRole != "source" {
			t.Fatalf("accessRole = %q, want source", accessRole)
		}
		return map[string]int{}, nil
	}

	stcls := &schemaTable{
		table:       "sbtest.*",
		rawTables:   "sbtest.*",
		sourceDrive: "mysql",
		datafix:     "table",
	}
	got, err := stcls.SchemaTableFilter(3, 4)
	if err != nil {
		t.Fatalf("SchemaTableFilter returned error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("SchemaTableFilter returned %v, want empty", got)
	}
	if !reflect.DeepEqual(gotAccessTargets, []string{"sbtest.*"}) {
		t.Fatalf("privilege precheck targets = %v, want [sbtest.*]", gotAccessTargets)
	}

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log failed: %v", err)
	}
	logText := string(content)
	for _, want := range []string{
		"current source user may lack SELECT privilege",
		"SHOW GRANTS FOR CURRENT_USER()",
		"SHOW GRANTS FOR '<user>'@'<host>'",
		"GRANT SELECT ON `sbtest`.* TO '<source_user>'@'<host>';",
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("log does not contain %q: %s", want, logText)
		}
	}
}
