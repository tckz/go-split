package split

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplit1(t *testing.T) {
	param := SplitParam{
		Verbose:     Bool(false),
		Split:       Int(1),
		Parallelism: Int(1),
		Prefix:      String("out/file-"),
		Compress:    String(""),
	}

	s := NewSplitter()
	s.mkdirAll = func(path string, perm os.FileMode) error {
		assert.Equal(t, "out", path)
		return nil
	}
	s.createReader = func(fn string) (io.Reader, cleanupFunc, error) {
		assert.Equal(t, "in/file0", fn)
		return strings.NewReader(`line1
line2
line3
`), nop, nil
	}

	actual := bytes.NewBuffer([]byte{})
	s.createWriter = func(fn string, compress string) (io.Writer, cleanupFunc, error) {
		assert.Equal(t, "out/file-000", fn)
		assert.Equal(t, "", compress)
		return actual, nop, nil
	}
	s.Split([]string{
		"in/file0",
	}, param)

	assert.Equal(t, `line1
line2
line3
`, actual.String())
}

func TestSplit2(t *testing.T) {
	param := SplitParam{
		Verbose:     Bool(true),
		Split:       Int(1),
		Parallelism: Int(1),
		Prefix:      String("out/file-"),
		Compress:    String("gzip"),
	}

	s := NewSplitter()
	stderr := bytes.NewBuffer([]byte{})
	s.stderr = stderr
	s.mkdirAll = func(path string, perm os.FileMode) error {
		assert.Equal(t, "out", path)
		return nil
	}
	s.createReader = func(fn string) (io.Reader, cleanupFunc, error) {
		assert.Equal(t, "in/file0", fn)
		return strings.NewReader(`line1
line2
line3
`), nop, nil
	}

	actual := bytes.NewBuffer([]byte{})
	s.createWriter = func(fn string, compress string) (io.Writer, cleanupFunc, error) {
		assert.Equal(t, "out/file-000.gz", fn)
		assert.Equal(t, "gzip", compress)
		return actual, nop, nil
	}
	s.Split([]string{
		"in/file0",
	}, param)

	assert.Equal(t, `line1
line2
line3
`, actual.String())

	assert.Equal(t, `in/file0
in/file0, total=3
`, stderr.String())
}
