package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCheckLockFile_NotExist tests checkLockFile when lock file does not exist
func TestCheckLockFile_NotExist(t *testing.T) {
	tmpDir := t.TempDir()
	lockPath := filepath.Join(tmpDir, ".repairDB.lock")

	err := checkLockFile(lockPath)
	if err != nil {
		t.Errorf("Expected no error when lock file does not exist, got: %v", err)
	}
}

// TestCheckLockFile_Empty tests checkLockFile when lock file exists and is empty
func TestCheckLockFile_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	lockPath := filepath.Join(tmpDir, ".repairDB.lock")

	// Create empty lock file
	if err := os.WriteFile(lockPath, []byte{}, 0644); err != nil {
		t.Fatalf("Failed to create test lock file: %v", err)
	}

	err := checkLockFile(lockPath)
	if err == nil {
		t.Error("Expected error when empty lock file exists, got nil")
	}

	if !strings.Contains(err.Error(), "previous execution completed successfully") {
		t.Errorf("Expected error message about successful execution, got: %v", err)
	}
}

// TestCheckLockFile_WithError tests checkLockFile when lock file contains error
func TestCheckLockFile_WithError(t *testing.T) {
	tmpDir := t.TempDir()
	lockPath := filepath.Join(tmpDir, ".repairDB.lock")
	errorMsg := "database connection failed"

	// Create lock file with error message
	if err := os.WriteFile(lockPath, []byte(errorMsg), 0644); err != nil {
		t.Fatalf("Failed to create test lock file: %v", err)
	}

	err := checkLockFile(lockPath)
	if err == nil {
		t.Error("Expected error when lock file with error exists, got nil")
	}

	if !strings.Contains(err.Error(), errorMsg) {
		t.Errorf("Expected error message to contain '%s', got: %v", errorMsg, err)
	}
}

// TestWriteLockFile_Success tests writeLockFile with empty error message (success case)
func TestWriteLockFile_Success(t *testing.T) {
	tmpDir := t.TempDir()
	lockPath := filepath.Join(tmpDir, ".repairDB.lock")

	err := writeLockFile(lockPath, "")
	if err != nil {
		t.Errorf("Expected no error when writing empty lock file, got: %v", err)
	}

	// Verify file exists and is empty
	content, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("Failed to read lock file: %v", err)
	}

	if len(content) != 0 {
		t.Errorf("Expected empty lock file, got content: %s", string(content))
	}
}

// TestWriteLockFile_Error tests writeLockFile with error message
func TestWriteLockFile_Error(t *testing.T) {
	tmpDir := t.TempDir()
	lockPath := filepath.Join(tmpDir, ".repairDB.lock")
	errorMsg := "execution failed: connection timeout"

	err := writeLockFile(lockPath, errorMsg)
	if err != nil {
		t.Errorf("Expected no error when writing lock file with error, got: %v", err)
	}

	// Verify file exists and contains error message
	content, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("Failed to read lock file: %v", err)
	}

	if string(content) != errorMsg {
		t.Errorf("Expected lock file content '%s', got: %s", errorMsg, string(content))
	}
}
