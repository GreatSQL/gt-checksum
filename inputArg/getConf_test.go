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

func TestGetConfig_DataDatafixTableGenRollSQLPreparesRollFileDir(t *testing.T) {
	workDir := t.TempDir()
	rollDir := workDir + "/custom-rollsql"
	configPath := workDir + "/gc.conf"
	config := strings.Join([]string{
		"srcDSN=mysql|user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
		"dstDSN=mysql|user:pass@tcp(127.0.0.1:3307)/information_schema?charset=utf8mb4",
		"tables=gt_checksum.*",
		"checkObject=data",
		"datafix=table",
		"genRollSQL=ON",
		"rollFileDir=" + rollDir,
		"resume=OFF",
	}, "\n") + "\n"
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	rc := &ConfigParameter{Config: configPath}
	rc.GetConfig()

	if rc.SecondaryL.RepairV.RollFileDir != rollDir {
		t.Fatalf("RollFileDir = %q, want %q", rc.SecondaryL.RepairV.RollFileDir, rollDir)
	}
	if _, err := os.Stat(rollDir); err != nil {
		t.Fatalf("expected rollback SQL directory to be created: %v", err)
	}
}

func TestGetConfig_DataDatafixTableGenRollSQLOffSkipsRollFileDir(t *testing.T) {
	workDir := t.TempDir()
	rollDir := workDir + "/custom-rollsql"
	configPath := workDir + "/gc.conf"
	config := strings.Join([]string{
		"srcDSN=mysql|user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
		"dstDSN=mysql|user:pass@tcp(127.0.0.1:3307)/information_schema?charset=utf8mb4",
		"tables=gt_checksum.*",
		"checkObject=data",
		"datafix=table",
		"genRollSQL=OFF",
		"rollFileDir=" + rollDir,
		"resume=OFF",
	}, "\n") + "\n"
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	rc := &ConfigParameter{Config: configPath}
	rc.GetConfig()

	if rc.SecondaryL.RepairV.RollFileDir != "" {
		t.Fatalf("RollFileDir = %q, want empty when genRollSQL=OFF", rc.SecondaryL.RepairV.RollFileDir)
	}
	if _, err := os.Stat(rollDir); !os.IsNotExist(err) {
		t.Fatalf("rollback SQL directory should not be created when genRollSQL=OFF, stat err=%v", err)
	}
}

func TestGetConfig_StructDatafixTableGenRollSQLSkipsRollFileDir(t *testing.T) {
	workDir := t.TempDir()
	fixDir := workDir + "/custom-fixsql"
	rollDir := workDir + "/custom-rollsql"
	configPath := workDir + "/gc.conf"
	config := strings.Join([]string{
		"srcDSN=mysql|user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
		"dstDSN=mysql|user:pass@tcp(127.0.0.1:3307)/information_schema?charset=utf8mb4",
		"tables=gt_checksum.*",
		"checkObject=struct",
		"datafix=table",
		"fixFileDir=" + fixDir,
		"genRollSQL=ON",
		"rollFileDir=" + rollDir,
		"resume=OFF",
	}, "\n") + "\n"
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	rc := &ConfigParameter{Config: configPath}
	rc.GetConfig()

	if rc.SecondaryL.RepairV.RollFileDir != "" {
		t.Fatalf("RollFileDir = %q, want empty for checkObject=struct", rc.SecondaryL.RepairV.RollFileDir)
	}
	if _, err := os.Stat(rollDir); !os.IsNotExist(err) {
		t.Fatalf("rollback SQL directory should not be created for checkObject=struct, stat err=%v", err)
	}
}

func TestGetConfig_DataDatafixFileGenRollSQLStillPreparesRollFileDir(t *testing.T) {
	workDir := t.TempDir()
	fixDir := workDir + "/custom-fixsql"
	rollDir := workDir + "/custom-rollsql"
	configPath := workDir + "/gc.conf"
	config := strings.Join([]string{
		"srcDSN=mysql|user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
		"dstDSN=mysql|user:pass@tcp(127.0.0.1:3307)/information_schema?charset=utf8mb4",
		"tables=gt_checksum.*",
		"checkObject=data",
		"datafix=file",
		"fixFileDir=" + fixDir,
		"genRollSQL=ON",
		"rollFileDir=" + rollDir,
		"resume=OFF",
	}, "\n") + "\n"
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	rc := &ConfigParameter{Config: configPath}
	rc.GetConfig()

	if rc.SecondaryL.RepairV.RollFileDir != rollDir {
		t.Fatalf("RollFileDir = %q, want %q", rc.SecondaryL.RepairV.RollFileDir, rollDir)
	}
	if _, err := os.Stat(rollDir); err != nil {
		t.Fatalf("expected rollback SQL directory to be created for datafix=file: %v", err)
	}
}

func TestGetConfig_TruncateBeforeAlterDefaultAndNormalization(t *testing.T) {
	tests := []struct {
		name string
		line string
		want string
	}{
		{name: "default off", want: "OFF"},
		{name: "upper on", line: "truncateBeforeAlter=ON", want: "ON"},
		{name: "lower on", line: "truncateBeforeAlter=on", want: "ON"},
		{name: "lower off", line: "truncateBeforeAlter=off", want: "OFF"},
		{name: "invalid passed through", line: "truncateBeforeAlter=maybe", want: "MAYBE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workDir := t.TempDir()
			configPath := workDir + "/gc.conf"
			lines := []string{
				"srcDSN=mysql|user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
				"dstDSN=mysql|user:pass@tcp(127.0.0.1:3307)/information_schema?charset=utf8mb4",
				"tables=gt_checksum.*",
				"checkObject=data",
				"datafix=table",
				"resume=OFF",
			}
			if tt.line != "" {
				lines = append(lines, tt.line)
			}
			config := strings.Join(lines, "\n") + "\n"
			if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
				t.Fatalf("failed to write temp config: %v", err)
			}

			rc := &ConfigParameter{Config: configPath}
			rc.GetConfig()

			if rc.SecondaryL.RepairV.TruncateBeforeAlter != tt.want {
				t.Fatalf("TruncateBeforeAlter = %q, want %q", rc.SecondaryL.RepairV.TruncateBeforeAlter, tt.want)
			}
		})
	}
}
