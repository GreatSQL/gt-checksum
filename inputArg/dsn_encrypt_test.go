package inputArg

import (
	"encoding/base64"
	"fmt"
	"gt-checksum/connstr"
	"testing"
)

func testDSNKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, connstr.KeySize)
	for i := range key {
		key[i] = byte(i + 1)
	}
	t.Setenv(connstr.EnvKeyName, base64.StdEncoding.EncodeToString(key))
	return key
}

func testEncryptedPassword(t *testing.T, password string) string {
	t.Helper()
	ciphertext, err := connstr.EncryptPassword(password, testDSNKey(t), "default")
	if err != nil {
		t.Fatalf("EncryptPassword() error = %v", err)
	}
	return ciphertext
}

func testEncryptedMySQLDSN(t *testing.T, addr string) string {
	t.Helper()
	return fmt.Sprintf("mysql|user:%s@tcp(%s)/information_schema?charset=utf8mb4", testEncryptedPassword(t, "pass"), addr)
}
