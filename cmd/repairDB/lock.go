package main

import (
	"fmt"
	"os"
)

const lockFileName = ".repairDB.lock"

// checkLockFile checks if the lock file exists and returns an error if it does.
func checkLockFile(lockPath string) error {
	if _, err := os.Stat(lockPath); err == nil {
		content, readErr := os.ReadFile(lockPath)
		if readErr != nil {
			return fmt.Errorf("lock file exists at %s but failed to read: %v", lockPath, readErr)
		}

		if len(content) == 0 {
			return fmt.Errorf("lock file exists at %s (previous execution completed successfully). Please remove it before running again", lockPath)
		}

		return fmt.Errorf("lock file exists at %s with error: %s. Please remove it before running again", lockPath, string(content))
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to check lock file at %s: %v", lockPath, err)
	}

	return nil
}

// writeLockFile writes the lock file with the given error message.
// If errMsg is empty, it creates an empty file (indicating success).
func writeLockFile(lockPath string, errMsg string) error {
	var content []byte
	if errMsg != "" {
		content = []byte(errMsg)
	}

	if err := os.WriteFile(lockPath, content, 0644); err != nil {
		return fmt.Errorf("failed to write lock file at %s: %v", lockPath, err)
	}

	return nil
}
