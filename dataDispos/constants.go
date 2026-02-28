package dataDispos

const (
	// ValueNullPlaceholder is the canonical marker for SQL NULL values in row comparison streams.
	ValueNullPlaceholder = "<nil>"
	// ValueEmptyPlaceholder is the canonical marker for empty string values in row comparison streams.
	ValueEmptyPlaceholder = "<entry>"
	// StreamEndMarker is the key used by merge streams to indicate stream completion.
	StreamEndMarker = "END"
	// StreamEndValue is the default payload for StreamEndMarker.
	StreamEndValue = "0"
)
