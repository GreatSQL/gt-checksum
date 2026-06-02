package inputArg

import (
	"os"
	"strings"
	"testing"
)

func TestGetConfig_StructDatafixTableReadsFixFileDir(t *testing.T) {
	workDir := t.TempDir()
	fixDir := workDir + "/custom-fixsql"
	configPath := workDir + "/gc.conf"
	config := strings.Join([]string{
		"srcDSN=mysql|user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
		"dstDSN=mysql|user:pass@tcp(127.0.0.1:3307)/information_schema?charset=utf8mb4",
		"tables=gt_checksum.*",
		"checkObject=struct",
		"datafix=table",
		"fixFileDir=" + fixDir,
		"resume=OFF",
	}, "\n") + "\n"
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	rc := &ConfigParameter{Config: configPath}
	rc.GetConfig()

	if rc.SecondaryL.RepairV.FixFileDir != fixDir {
		t.Fatalf("FixFileDir = %q, want %q", rc.SecondaryL.RepairV.FixFileDir, fixDir)
	}
	if _, err := os.Stat(fixDir); err != nil {
		t.Fatalf("expected forced fix SQL directory to be created: %v", err)
	}
}

func TestGetConfig_StructDatafixTableUsesDefaultFixFileDir(t *testing.T) {
	workDir := t.TempDir()
	configPath := workDir + "/gc.conf"
	config := strings.Join([]string{
		"srcDSN=mysql|user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
		"dstDSN=mysql|user:pass@tcp(127.0.0.1:3307)/information_schema?charset=utf8mb4",
		"tables=gt_checksum.*",
		"checkObject=struct",
		"datafix=table",
		"resume=OFF",
	}, "\n") + "\n"
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("Chdir(%s) failed: %v", workDir, err)
	}
	defer func() {
		if err := os.Chdir(oldWd); err != nil {
			t.Fatalf("restore working directory failed: %v", err)
		}
	}()

	rc := &ConfigParameter{Config: configPath}
	rc.GetConfig()

	if rc.SecondaryL.RepairV.FixFileDir != "fixsql" {
		t.Fatalf("FixFileDir = %q, want default fixsql", rc.SecondaryL.RepairV.FixFileDir)
	}
	if _, err := os.Stat(workDir + "/fixsql"); err != nil {
		t.Fatalf("expected default forced fix SQL directory to be created: %v", err)
	}
}
