package actions

import (
	"strings"
	"testing"
)

func TestRollbackDeleteToInsert(t *testing.T) {
	tests := []struct {
		name       string
		deleteSQL  string
		schema     string
		table      string
		wantEmpty  bool
		wantInsert bool
	}{
		{
			name:       "pri delete no limit",
			deleteSQL:  "DELETE FROM `sbtest`.`t2` WHERE `id` = '42';",
			schema:     "sbtest",
			table:      "t2",
			wantInsert: true,
		},
		{
			name:       "mul delete with limit",
			deleteSQL:  "DELETE FROM `sbtest`.`t2` WHERE `id` = '42' LIMIT 1;",
			schema:     "sbtest",
			table:      "t2",
			wantInsert: true,
		},
		{
			name:      "no where clause",
			deleteSQL: "DELETE FROM `sbtest`.`t2`;",
			schema:    "sbtest",
			table:     "t2",
			wantEmpty: true,
		},
		{
			name:       "composite pk",
			deleteSQL:  "DELETE FROM `sbtest`.`t2` WHERE `a` = '1' AND `b` = '2';",
			schema:     "sbtest",
			table:      "t2",
			wantInsert: true,
		},
		{
			name:       "null value",
			deleteSQL:  "DELETE FROM `sbtest`.`t2` WHERE `name` IS NULL;",
			schema:     "sbtest",
			table:      "t2",
			wantInsert: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rollbackDeleteToInsert(tt.deleteSQL, tt.schema, tt.table)
			if tt.wantEmpty {
				if got != "" {
					t.Errorf("expected empty, got %q", got)
				}
				return
			}
			if tt.wantEmpty && got != "" {
				t.Errorf("expected empty, got %q", got)
				return
			}
			if tt.wantInsert && got == "" {
				t.Errorf("expected non-empty INSERT, got empty")
				return
			}
			if tt.wantInsert && !strings.HasPrefix(got, "INSERT INTO") {
				t.Errorf("expected INSERT INTO, got %q", got)
			}
		})
	}
}

func TestRollbackInsertToDelete(t *testing.T) {
	tests := []struct {
		name         string
		insertSQL    string
		schema       string
		table        string
		indexColumns []string
		wantEmpty    bool
		wantDelete   bool
	}{
		{
			name:         "single pk",
			insertSQL:    "INSERT INTO `sbtest`.`t2`(`id`,`name`,`val`) VALUES('42','hello','100');",
			schema:       "sbtest",
			table:        "t2",
			indexColumns: []string{"id"},
			wantDelete:   true,
		},
		{
			name:         "composite pk",
			insertSQL:    "INSERT INTO `sbtest`.`t2`(`a`,`b`,`c`) VALUES('1','2','3');",
			schema:       "sbtest",
			table:        "t2",
			indexColumns: []string{"a", "b"},
			wantDelete:   true,
		},
		{
			name:         "null value in pk",
			insertSQL:    "INSERT INTO `sbtest`.`t2`(`id`,`name`) VALUES(NULL,'hello');",
			schema:       "sbtest",
			table:        "t2",
			indexColumns: []string{"id"},
			wantDelete:   true,
		},
		{
			name:         "no matching pk columns",
			insertSQL:    "INSERT INTO `sbtest`.`t2`(`id`,`name`) VALUES('42','hello');",
			schema:       "sbtest",
			table:        "t2",
			indexColumns: []string{"nonexistent"},
			wantEmpty:    true,
		},
		{
			name:         "empty insert",
			insertSQL:    "",
			schema:       "sbtest",
			table:        "t2",
			indexColumns: []string{"id"},
			wantEmpty:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rollbackInsertToDelete(tt.insertSQL, tt.schema, tt.table, tt.indexColumns)
			if tt.wantEmpty {
				if got != "" {
					t.Errorf("expected empty, got %q", got)
				}
				return
			}
			if tt.wantDelete && got == "" {
				t.Errorf("expected non-empty DELETE, got empty")
				return
			}
			if tt.wantDelete && !strings.HasPrefix(got, "DELETE FROM") {
				t.Errorf("expected DELETE FROM, got %q", got)
			}
		})
	}
}

func TestRollbackRowToInsert(t *testing.T) {
	colData := []map[string]string{
		{"columnName": "id", "dataType": "int"},
		{"columnName": "name", "dataType": "varchar"},
	}

	tests := []struct {
		name    string
		rowData string
		want    string
	}{
		{
			name:    "normal row",
			rowData: "42/*go actions columnData*/hello",
			want:    "INSERT INTO `s`.`t`(`id`,`name`) VALUES('42','hello');",
		},
		{
			name:    "nil value",
			rowData: "<nil>/*go actions columnData*/hello",
			want:    "INSERT INTO `s`.`t`(`id`,`name`) VALUES(NULL,'hello');",
		},
		{
			name:    "empty entry",
			rowData: "<entry>/*go actions columnData*/hello",
			want:    "INSERT INTO `s`.`t`(`id`,`name`) VALUES('' ,'hello');",
		},
		{
			name:    "empty row",
			rowData: "",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rollbackRowToInsert("s", "t", tt.rowData, colData)
			if tt.want == "" {
				if got != "" {
					t.Errorf("expected empty, got %q", got)
				}
				return
			}
			if got == "" {
				t.Errorf("expected non-empty, got empty")
				return
			}
			// For normal row, verify structure
			if tt.name == "normal row" && !strings.HasPrefix(got, "INSERT INTO") {
				t.Errorf("expected INSERT INTO, got %q", got)
			}
		})
	}
}

func TestRollbackRowToDelete(t *testing.T) {
	colData := []map[string]string{
		{"columnName": "id", "dataType": "int"},
		{"columnName": "name", "dataType": "varchar"},
	}

	tests := []struct {
		name         string
		rowData      string
		indexColumns []string
		want         string
		wantEmpty    bool
	}{
		{
			name:         "normal row",
			rowData:      "42/*go actions columnData*/hello",
			indexColumns: []string{"id"},
			want:         "DELETE FROM `s`.`t` WHERE `id` = '42';",
		},
		{
			name:         "nil value",
			rowData:      "<nil>/*go actions columnData*/hello",
			indexColumns: []string{"id"},
			want:         "DELETE FROM `s`.`t` WHERE `id` IS NULL;",
		},
		{
			name:         "empty entry",
			rowData:      "<entry>/*go actions columnData*/hello",
			indexColumns: []string{"id"},
			want:         "DELETE FROM `s`.`t` WHERE `id` = '';",
		},
		{
			name:         "empty row",
			rowData:      "",
			indexColumns: []string{"id"},
			wantEmpty:    true,
		},
		{
			name:         "no index columns",
			rowData:      "42/*go actions columnData*/hello",
			indexColumns: []string{},
			wantEmpty:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rollbackRowToDelete("s", "t", tt.rowData, colData, tt.indexColumns)
			if tt.wantEmpty {
				if got != "" {
					t.Errorf("expected empty, got %q", got)
				}
				return
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMatchRollSQLTarget(t *testing.T) {
	tests := []struct {
		genRollSQL string
		schema     string
		table      string
		want       bool
	}{
		{"OFF", "gt_checksum", "test1", false},
		{"off", "gt_checksum", "test1", false},
		{"", "gt_checksum", "test1", false},
		{"ON", "gt_checksum", "test1", true},
		{"on", "gt_checksum", "test1", true},
		// exact match
		{"gt_checksum.test1", "gt_checksum", "test1", true},
		{"gt_checksum.test1", "gt_checksum", "test2", false},
		// wildcard
		{"gt_checksum.test%", "gt_checksum", "test1", true},
		{"gt_checksum.test%", "gt_checksum", "testABC", true},
		{"gt_checksum.test%", "gt_checksum", "other", false},
		// multi-table
		{"gt_checksum.test1, gt_checksum.test2", "gt_checksum", "test1", true},
		{"gt_checksum.test1, gt_checksum.test2", "gt_checksum", "test2", true},
		{"gt_checksum.test1, gt_checksum.test2", "gt_checksum", "test3", false},
		// mixed wildcard and exact
		{"gt_checksum.test%, other_db.tbl", "other_db", "tbl", true},
		{"gt_checksum.test%, other_db.tbl", "other_db", "tbl2", false},
		// 回归：自定义表名保留原始大小写，path.Match 大小写敏感，不应被 ToUpper 破坏
		{"gt_checksum.test1", "gt_checksum", "test1", true},
		{"GT_CHECKSUM.TEST1", "gt_checksum", "test1", false}, // 大写 pattern 不匹配小写 target
	}
	for _, tt := range tests {
		got := matchRollSQLTarget(tt.genRollSQL, tt.schema, tt.table)
		if got != tt.want {
			t.Errorf("matchRollSQLTarget(%q, %q, %q) = %v, want %v",
				tt.genRollSQL, tt.schema, tt.table, got, tt.want)
		}
	}
}
