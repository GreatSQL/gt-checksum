package actions

import (
	"fmt"
	"strings"
)

// fixFileNameEncode encodes a schema or object name for safe use as a filename
// component in the type.schema.object.sql naming convention.
//
// Characters in [A-Za-z0-9_-] are kept as-is; all other characters (including
// '.', ' ', '`', '/', '\', and multi-byte UTF-8 runes) are percent-encoded as
// %XX using uppercase hex digits.  The encoding is stable and reversible.
func fixFileNameEncode(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
