package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestChecksumProgressResultDir(t *testing.T) {
	tmpDir := t.TempDir()
	existingResultDir := filepath.Join(tmpDir, "result")
	if err := os.Mkdir(existingResultDir, 0755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	cases := []struct {
		name       string
		resultFile string
		want       string
	}{
		{name: "empty uses default", resultFile: "", want: "result"},
		{name: "default result directory", resultFile: "result", want: "result"},
		{name: "existing directory", resultFile: existingResultDir, want: existingResultDir},
		{name: "trailing separator directory", resultFile: existingResultDir + string(os.PathSeparator), want: existingResultDir},
		{name: "explicit csv file", resultFile: filepath.Join(tmpDir, "out", "result.csv"), want: filepath.Join(tmpDir, "out")},
		{name: "bare csv filename keeps default progress directory", resultFile: "result.csv", want: "result"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := checksumProgressResultDir(tc.resultFile); got != tc.want {
				t.Fatalf("checksumProgressResultDir(%q) = %q, want %q", tc.resultFile, got, tc.want)
			}
		})
	}
}
