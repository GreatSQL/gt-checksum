package actions

import (
	"bytes"
	"gt-checksum/inputArg"
	"io"
	"os"
	"strings"
	"testing"
)

func captureCheckResultOut(t *testing.T, m *inputArg.ConfigParameter) string {
	t.Helper()
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	CheckResultOut(m)

	if err := w.Close(); err != nil {
		t.Fatalf("close stdout pipe failed: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read stdout pipe failed: %v", err)
	}
	return buf.String()
}

func withTerminalResultState(t *testing.T, pods []Pod, mappings []string) {
	t.Helper()
	originalPods := measuredDataPods
	originalMappings := TableMappingRelations
	measuredDataPods = pods
	TableMappingRelations = mappings
	t.Cleanup(func() {
		measuredDataPods = originalPods
		TableMappingRelations = originalMappings
	})
}

func TestCheckResultOut_DatafixTableShowsFixed(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	m.SecondaryL.RulesV.CheckObject = "data"
	m.SecondaryL.RepairV.Datafix = "table"
	withTerminalResultState(t, []Pod{{Schema: "sbtest", Table: "t1", IndexColumn: "id", CheckObject: "data", Rows: "10,9", DIFFS: "yes", Datafix: "table", Fixed: "yes"}}, nil)

	out := captureCheckResultOut(t, m)
	if !strings.Contains(out, "Fixed") {
		t.Fatalf("datafix=table output should contain Fixed header, got:\n%s", out)
	}
	if !strings.Contains(out, "yes") {
		t.Fatalf("datafix=table output should contain Fixed=yes, got:\n%s", out)
	}
}

func TestCheckResultOut_DatafixFileDoesNotShowFixed(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	m.SecondaryL.RulesV.CheckObject = "data"
	m.SecondaryL.RepairV.Datafix = "file"
	withTerminalResultState(t, []Pod{{Schema: "sbtest", Table: "t1", IndexColumn: "id", CheckObject: "data", Rows: "10,9", DIFFS: "yes", Datafix: "file", Fixed: "yes"}}, nil)

	out := captureCheckResultOut(t, m)
	if strings.Contains(out, "Fixed") {
		t.Fatalf("datafix=file output should not contain Fixed header, got:\n%s", out)
	}
}

func TestCheckResultOut_DatafixTableShowsFixedWithMappingAndColumns(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	m.SecondaryL.RulesV.CheckObject = "data"
	m.SecondaryL.RepairV.Datafix = "table"
	withTerminalResultState(t, []Pod{{Schema: "src", Table: "t1", IndexColumn: "id", CheckObject: "data", Rows: "10,10", DIFFS: "no", Datafix: "table", Fixed: "no", MappingInfo: "Schema: src:dst", ColumnsInfo: "src.t1.c1"}}, []string{"src.t1:dst.t1"})

	out := captureCheckResultOut(t, m)
	for _, want := range []string{"Datafix", "Fixed", "Mapping", "Columns", "no", "skipped", "Schema: src:dst", "src.t1.c1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q, got:\n%s", want, out)
		}
	}
}
