package split

//go:generate stringer -type=CompressionType $GOFILE
type CompressionType int

const (
	CompressionUnknown CompressionType = iota
	CompressionNone
	CompressionGzip
)

type compressionTypeInfo struct {
	id        CompressionType
	extension string
}

var compressionMap = map[string]compressionTypeInfo{
	"":     {CompressionNone, ""},
	"none": {CompressionNone, ""},
	"gzip": {CompressionGzip, ".gz"},
	"gz":   {CompressionGzip, ".gz"},
}

func getCompressionType(compression string) (CompressionType, string) {
	if t, ok := compressionMap[compression]; !ok {
		return CompressionUnknown, ""
	} else {
		return t.id, t.extension
	}
}
