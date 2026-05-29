package main

import (
	"fmt"
	"testing"
)

func TestIsDuplicateKeyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "mysql error code", err: fmt.Errorf("Error 1062: Duplicate entry '1' for key 'PRIMARY'"), want: true},
		{name: "duplicate text", err: fmt.Errorf("Duplicate entry '1' for key 'PRIMARY'"), want: true},
		{name: "other error", err: fmt.Errorf("Error 1213: Deadlock found when trying to get lock"), want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDuplicateKeyError(tc.err); got != tc.want {
				t.Fatalf("isDuplicateKeyError() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSplitMultiValueInsert_Basic(t *testing.T) {
	stmt := "INSERT INTO `db`.`t`(`id`, `name`) VALUES (1,'a'),(2,'b');"
	got, ok, err := splitMultiValueInsert(stmt)
	if err != nil {
		t.Fatalf("splitMultiValueInsert returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected INSERT to be split")
	}
	want := []string{
		"INSERT INTO `db`.`t`(`id`, `name`) VALUES (1,'a');",
		"INSERT INTO `db`.`t`(`id`, `name`) VALUES (2,'b');",
	}
	assertSplitInsertSQLs(t, got, want)
}

func TestSplitMultiValueInsert_NoColumns(t *testing.T) {
	stmt := "INSERT INTO `db`.`t` VALUES (1,'a'),(2,'b');"
	got, ok, err := splitMultiValueInsert(stmt)
	if err != nil {
		t.Fatalf("splitMultiValueInsert returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected INSERT to be split")
	}
	want := []string{
		"INSERT INTO `db`.`t` VALUES (1,'a');",
		"INSERT INTO `db`.`t` VALUES (2,'b');",
	}
	assertSplitInsertSQLs(t, got, want)
}

func TestSplitMultiValueInsert_QuotedCommasAndParentheses(t *testing.T) {
	stmt := "INSERT INTO `db`.`t`(`id`,`name`) VALUES (1,'a,b'),(2,'x(y)'),(3,'it\\'s ok');"
	got, ok, err := splitMultiValueInsert(stmt)
	if err != nil {
		t.Fatalf("splitMultiValueInsert returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected INSERT to be split")
	}
	want := []string{
		"INSERT INTO `db`.`t`(`id`,`name`) VALUES (1,'a,b');",
		"INSERT INTO `db`.`t`(`id`,`name`) VALUES (2,'x(y)');",
		"INSERT INTO `db`.`t`(`id`,`name`) VALUES (3,'it\\'s ok');",
	}
	assertSplitInsertSQLs(t, got, want)
}

func TestSplitMultiValueInsert_FunctionValues(t *testing.T) {
	stmt := "INSERT INTO `db`.`t`(`id`,`p`,`payload`) VALUES (1,POINT(1,2),JSON_OBJECT('a',1)),(2,POINT(3,4),JSON_OBJECT('b',2));"
	got, ok, err := splitMultiValueInsert(stmt)
	if err != nil {
		t.Fatalf("splitMultiValueInsert returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected INSERT to be split")
	}
	want := []string{
		"INSERT INTO `db`.`t`(`id`,`p`,`payload`) VALUES (1,POINT(1,2),JSON_OBJECT('a',1));",
		"INSERT INTO `db`.`t`(`id`,`p`,`payload`) VALUES (2,POINT(3,4),JSON_OBJECT('b',2));",
	}
	assertSplitInsertSQLs(t, got, want)
}

func TestSplitMultiValueInsert_Multiline(t *testing.T) {
	stmt := "INSERT INTO `db`.`t`(`id`,`name`) VALUES\n(1,'a'),\n(2,'b');"
	got, ok, err := splitMultiValueInsert(stmt)
	if err != nil {
		t.Fatalf("splitMultiValueInsert returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected INSERT to be split")
	}
	want := []string{
		"INSERT INTO `db`.`t`(`id`,`name`) VALUES (1,'a');",
		"INSERT INTO `db`.`t`(`id`,`name`) VALUES (2,'b');",
	}
	assertSplitInsertSQLs(t, got, want)
}

func TestSplitMultiValueInsert_LeadingComment(t *testing.T) {
	stmt := "-- chunk 1\nINSERT INTO `db`.`t`(`id`) VALUES (1),(2);"
	got, ok, err := splitMultiValueInsert(stmt)
	if err != nil {
		t.Fatalf("splitMultiValueInsert returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected INSERT with leading comment to be split")
	}
	want := []string{
		"INSERT INTO `db`.`t`(`id`) VALUES (1);",
		"INSERT INTO `db`.`t`(`id`) VALUES (2);",
	}
	assertSplitInsertSQLs(t, got, want)
}

func TestSplitMultiValueInsert_CommentsInValues(t *testing.T) {
	stmt := "INSERT INTO `db`.`t`(`id`,`name`) VALUES (1, /* ) */ 'a') /* next */, (2,'b');"
	got, ok, err := splitMultiValueInsert(stmt)
	if err != nil {
		t.Fatalf("splitMultiValueInsert returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected INSERT with comments to be split")
	}
	want := []string{
		"INSERT INTO `db`.`t`(`id`,`name`) VALUES (1, /* ) */ 'a');",
		"INSERT INTO `db`.`t`(`id`,`name`) VALUES (2,'b');",
	}
	assertSplitInsertSQLs(t, got, want)
}

func TestSplitMultiValueInsert_NotSplittable(t *testing.T) {
	cases := []string{
		"INSERT INTO `db`.`t`(`id`) VALUES (1);",
		"INSERT INTO `db`.`t`(`id`) SELECT id FROM `db`.`s`;",
		"DELETE FROM `db`.`t` WHERE `id` = 1;",
		"INSERT INTO `db`.`t`(`id`) VALUES (1),(2) ON DUPLICATE KEY UPDATE `id` = VALUES(`id`);",
	}

	for _, stmt := range cases {
		t.Run(stmt, func(t *testing.T) {
			got, ok, err := splitMultiValueInsert(stmt)
			if err != nil {
				t.Fatalf("splitMultiValueInsert returned error: %v", err)
			}
			if ok || len(got) != 0 {
				t.Fatalf("expected not splittable, ok=%v got=%v", ok, got)
			}
		})
	}
}

func TestSplitMultiValueInsert_InvalidTuple(t *testing.T) {
	_, ok, err := splitMultiValueInsert("INSERT INTO `db`.`t`(`id`) VALUES (1,(2);")
	if err == nil {
		t.Fatal("expected invalid tuple error")
	}
	if ok {
		t.Fatal("expected invalid INSERT not to be marked splittable")
	}
}

func assertSplitInsertSQLs(t *testing.T, got []splitInsertStatement, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d statements, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].tupleIndex != i+1 {
			t.Fatalf("statement %d tupleIndex = %d, want %d", i, got[i].tupleIndex, i+1)
		}
		if got[i].sql != want[i] {
			t.Fatalf("statement %d SQL = %q, want %q", i, got[i].sql, want[i])
		}
	}
}
