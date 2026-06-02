package actions

import (
	"reflect"
	"strings"
	"testing"
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
