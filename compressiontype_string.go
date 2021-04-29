// Code generated by "stringer -type=CompressionType compression_type.go"; DO NOT EDIT.

package split

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[CompressionUnknown-0]
	_ = x[CompressionNone-1]
	_ = x[CompressionGzip-2]
}

const _CompressionType_name = "CompressionUnknownCompressionNoneCompressionGzip"

var _CompressionType_index = [...]uint8{0, 18, 33, 48}

func (i CompressionType) String() string {
	if i < 0 || i >= CompressionType(len(_CompressionType_index)-1) {
		return "CompressionType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _CompressionType_name[_CompressionType_index[i]:_CompressionType_index[i+1]]
}
