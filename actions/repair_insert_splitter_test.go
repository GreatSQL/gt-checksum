package actions

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type fakeRepairResult int64

func (r fakeRepairResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r fakeRepairResult) RowsAffected() (int64, error) {
	return int64(r), nil
}

type fakeRepairExecer struct {
	errs     map[string]error
	executed []string
}

func (f *fakeRepairExecer) ExecContext(_ context.Context, query string, _ ...interface{}) (sql.Result, error) {
	f.executed = append(f.executed, query)
	if err, ok := f.errs[query]; ok {
		return fakeRepairResult(0), err
	}
	return fakeRepairResult(1), nil
}

func TestExecRepairStatementWithDupKeySplitSkipsDuplicateTuples(t *testing.T) {
	original := "INSERT INTO `sbtest`.`t2` (`id`,`c`) VALUES (1,'a'),(2,'b'),(3,'c');"
	dupTuple := "INSERT INTO `sbtest`.`t2` (`id`,`c`) VALUES (2,'b');"
	dupErr := errors.New("Error 1062 (23000): Duplicate entry '2' for key 'PRIMARY'")
	execer := &fakeRepairExecer{errs: map[string]error{
		original: dupErr,
		dupTuple: dupErr,
	}}

	if err := execRepairStatementWithDupKeySplit(context.Background(), execer, original, 7, "test batch"); err != nil {
		t.Fatalf("execRepairStatementWithDupKeySplit returned error: %v", err)
	}

	want := []string{
		original,
		"INSERT INTO `sbtest`.`t2` (`id`,`c`) VALUES (1,'a');",
		dupTuple,
		"INSERT INTO `sbtest`.`t2` (`id`,`c`) VALUES (3,'c');",
	}
	assertExecutedSQL(t, execer.executed, want)
}

func TestExecRepairStatementWithDupKeySplitStopsOnNonDuplicateTupleError(t *testing.T) {
	original := "INSERT INTO `sbtest`.`t4` (`id`,`c`) VALUES (1,'a'),(2,'b'),(3,'c');"
	badTuple := "INSERT INTO `sbtest`.`t4` (`id`,`c`) VALUES (2,'b');"
	dupErr := errors.New("Error 1062 (23000): Duplicate entry '2' for key 'PRIMARY'")
	badErr := errors.New("Error 1146 (42S02): Table 'sbtest.t4' doesn't exist")
	execer := &fakeRepairExecer{errs: map[string]error{
		original: dupErr,
		badTuple: badErr,
	}}

	err := execRepairStatementWithDupKeySplit(context.Background(), execer, original, 8, "test batch")
	if err == nil {
		t.Fatalf("expected non-duplicate split error")
	}
	if !strings.Contains(err.Error(), "split INSERT tuple #2/3 failed") {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{
		original,
		"INSERT INTO `sbtest`.`t4` (`id`,`c`) VALUES (1,'a');",
		badTuple,
	}
	assertExecutedSQL(t, execer.executed, want)
}

func TestExecRepairStatementWithDupKeySplitKeepsOriginalErrorForSingleInsert(t *testing.T) {
	original := "INSERT INTO `sbtest`.`t2` (`id`) VALUES (1);"
	dupErr := errors.New("Error 1062 (23000): Duplicate entry '1' for key 'PRIMARY'")
	execer := &fakeRepairExecer{errs: map[string]error{
		original: dupErr,
	}}

	err := execRepairStatementWithDupKeySplit(context.Background(), execer, original, 9, "test batch")
	if err == nil {
		t.Fatalf("expected original duplicate error")
	}
	if err.Error() != dupErr.Error() {
		t.Fatalf("expected original duplicate error %q, got %q", dupErr.Error(), err.Error())
	}

	assertExecutedSQL(t, execer.executed, []string{original})
}

func assertExecutedSQL(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("executed SQL length = %d, want %d\ngot:  %#v\nwant: %#v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("executed SQL[%d] = %q, want %q\nall got: %#v", i, got[i], want[i], got)
		}
	}
}
