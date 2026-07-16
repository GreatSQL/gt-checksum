package actions

import (
	"gt-checksum/global"
	"testing"
)

func TestCheckXXHash64(t *testing.T) {
	cs := CheckSum()

	// Test basic functionality
	data := "hello world"
	hash1 := cs.CheckXXHash64(data)
	hash2 := cs.CheckXXHash64(data)

	if hash1 != hash2 {
		t.Errorf("CheckXXHash64 should return same hash for same input, got %s and %s", hash1, hash2)
	}

	// Test that hash is 16 hex characters (64 bits)
	if len(hash1) != 16 {
		t.Errorf("CheckXXHash64 should return 16 hex characters, got %d", len(hash1))
	}

	// Test different inputs produce different hashes
	hash3 := cs.CheckXXHash64("different data")
	if hash1 == hash3 {
		t.Errorf("CheckXXHash64 should return different hash for different input")
	}
}

func TestCheckMd5(t *testing.T) {
	cs := CheckSum()

	// Test basic functionality
	data := "hello world"
	hash1 := cs.CheckMd5(data)
	hash2 := cs.CheckMd5(data)

	if hash1 != hash2 {
		t.Errorf("CheckMd5 should return same hash for same input, got %s and %s", hash1, hash2)
	}

	// Test that hash is 32 hex characters (128 bits)
	if len(hash1) != 32 {
		t.Errorf("CheckMd5 should return 32 hex characters, got %d", len(hash1))
	}
}

func TestCheckHash_DefaultAlgorithm(t *testing.T) {
	cs := CheckSum()

	// Save original value and restore after test
	originalAlgo := global.HashAlgorithm
	defer func() {
		global.HashAlgorithm = originalAlgo
	}()

	// Test with xxhash64 (default)
	global.HashAlgorithm = "xxhash64"
	data := "test data"
	hashDefault := cs.CheckHash(data)
	hashXXHash := cs.CheckXXHash64(data)

	if hashDefault != hashXXHash {
		t.Errorf("CheckHash with xxhash64 should match CheckXXHash64, got %s and %s", hashDefault, hashXXHash)
	}
}

func TestCheckHash_MD5Algorithm(t *testing.T) {
	cs := CheckSum()

	// Save original value and restore after test
	originalAlgo := global.HashAlgorithm
	defer func() {
		global.HashAlgorithm = originalAlgo
	}()

	// Test with md5
	global.HashAlgorithm = "md5"
	data := "test data"
	hashMD5 := cs.CheckHash(data)
	hashExpected := cs.CheckMd5(data)

	if hashMD5 != hashExpected {
		t.Errorf("CheckHash with md5 should match CheckMd5, got %s and %s", hashMD5, hashExpected)
	}
}

func TestCompareHash(t *testing.T) {
	// Save original value and restore after test
	originalAlgo := global.HashAlgorithm
	defer func() {
		global.HashAlgorithm = originalAlgo
	}()

	data1 := []byte("hello world")
	data2 := []byte("hello world")
	data3 := []byte("different data")

	// Test with xxhash64
	global.HashAlgorithm = "xxhash64"
	if !compareHash(data1, data2) {
		t.Error("compareHash should return true for identical data with xxhash64")
	}
	if compareHash(data1, data3) {
		t.Error("compareHash should return false for different data with xxhash64")
	}

	// Test with md5
	global.HashAlgorithm = "md5"
	if !compareHash(data1, data2) {
		t.Error("compareHash should return true for identical data with md5")
	}
	if compareHash(data1, data3) {
		t.Error("compareHash should return false for different data with md5")
	}
}
