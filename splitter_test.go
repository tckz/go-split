package split

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type serviceMock struct {
	mockCreateWriter func(fn string, compress string) (io.Writer, cleanupFunc, error)
	mockCreateReader func(fn string) (io.ReadCloser, error)
	mockMkdirAll     func(path string, perm os.FileMode) error
}

func (m *serviceMock) createWriter(fn string, compress string) (io.Writer, cleanupFunc, error) {
	return m.mockCreateWriter(fn, compress)
}
func (m *serviceMock) createReader(fn string) (io.ReadCloser, error) {
	return m.mockCreateReader(fn)
}
func (m *serviceMock) mkdirAll(path string, perm os.FileMode) error {
	return m.mockMkdirAll(path, perm)
}

func TestSplit1(t *testing.T) {
	param := Param{
		Verbose:     false,
		Split:       1,
		Parallelism: 1,
		Prefix:      "out/file-",
		Compress:    "",
	}

	s := NewSplitter()
	actual := bytes.NewBuffer([]byte{})
	mock := &serviceMock{
		mockMkdirAll: func(path string, perm os.FileMode) error {
			assert.Equal(t, "out", path)
			return nil
		},
		mockCreateReader: func(fn string) (io.ReadCloser, error) {
			assert.Equal(t, "in/file0", fn)
			return io.NopCloser(strings.NewReader(`line1
line2
line3
`)), nil
		},
		mockCreateWriter: func(fn string, compress string) (io.Writer, cleanupFunc, error) {
			assert.Equal(t, "out/file-000", fn)
			assert.Equal(t, "", compress)
			return actual, nop, nil
		},
	}

	s.svc = mock

	s.Do(context.Background(), []string{
		"in/file0",
	}, param)

	assert.Equal(t, `line1
line2
line3
`, actual.String())
}

func TestSplit2(t *testing.T) {
	param := Param{
		Verbose:     true,
		Split:       1,
		Parallelism: 1,
		Prefix:      "out/file-",
		Compress:    "gzip",
	}

	s := NewSplitter()
	stderr := bytes.NewBuffer([]byte{})
	s.stderr = stderr

	actual := bytes.NewBuffer([]byte{})
	mock := &serviceMock{
		mockMkdirAll: func(path string, perm os.FileMode) error {
			assert.Equal(t, "out", path)
			return nil
		},
		mockCreateReader: func(fn string) (io.ReadCloser, error) {
			assert.Equal(t, "in/file0", fn)
			return io.NopCloser(strings.NewReader(`line1
line2
line3
`)), nil
		},
		mockCreateWriter: func(fn string, compress string) (io.Writer, cleanupFunc, error) {
			assert.Equal(t, "out/file-000.gz", fn)
			assert.Equal(t, "gzip", compress)
			return actual, nop, nil
		},
	}

	s.svc = mock

	s.Do(context.Background(), []string{
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

func TestDecorateReaderNoCompression(t *testing.T) {
	r, err := os.Open("/dev/stdin")
	assert.Nil(t, err)
	defer r.Close()

	reader, err := decorateReader("plain.txt", r)
	assert.Nil(t, err)
	defer reader.Close()
}

func TestDecorateReaderGzip(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	w := gzip.NewWriter(buf)
	defer w.Close()
	w.Write([]byte("aaa"))
	w.Close()

	r := bytes.NewReader(buf.Bytes())

	reader, err := decorateReader("path/to/some.tsv.gz", r)
	assert.Nil(t, err)
	defer reader.Close()

	_, ok := reader.(*gzip.Reader)
	assert.True(t, ok)

	content, err := io.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, "aaa", string(content))

}

func TestDecorateWriterNone(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	w, cleanup, err := decorateWriter("none", buf)
	assert.Nil(t, err)
	defer cleanup()

	assert.True(t, buf == w)
}

func TestDecorateWriterGzip(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	w, cleanup, err := decorateWriter("gzip", buf)
	assert.Nil(t, err)
	defer cleanup()

	_, ok := w.(*gzip.Writer)
	assert.True(t, ok)
}

func TestDecorateWriterUnknown(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	w, cleanup, err := decorateWriter("lzo", buf)
	assert.NotNil(t, err)
	assert.Nil(t, w)
	assert.Nil(t, cleanup)
}
