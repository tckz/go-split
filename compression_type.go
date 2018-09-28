package split

//go:generate stringer -type=CompressionType compression_type.go
type CompressionType int

const (
	CompressionUnknown CompressionType = iota
	CompressionNone
	CompressionGzip
)

var compressionMap = map[string]CompressionType{
	"":     CompressionNone,
	"gzip": CompressionGzip,
	"gz":   CompressionGzip,
}

func getCompressionType(compression string) CompressionType {
	if t, ok := compressionMap[compression]; !ok {
		return CompressionUnknown
	} else {
		return t
	}
}
