package split

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type serviceMock struct {
	mockCreateWriter func(fn string, compress string) (io.Writer, cleanupFunc, error)
	mockCreateReader func(fn string) (io.Reader, cleanupFunc, error)
	mockMkdirAll     func(path string, perm os.FileMode) error
}

func (m *serviceMock) createWriter(fn string, compress string) (io.Writer, cleanupFunc, error) {
	return m.mockCreateWriter(fn, compress)
}
func (m *serviceMock) createReader(fn string) (io.Reader, cleanupFunc, error) {
	return m.mockCreateReader(fn)
}
func (m *serviceMock) mkdirAll(path string, perm os.FileMode) error {
	return m.mockMkdirAll(path, perm)
}

func TestSplit1(t *testing.T) {
	param := Param{
		Verbose:     Bool(false),
		Split:       Int(1),
		Parallelism: Int(1),
		Prefix:      String("out/file-"),
		Compress:    String(""),
	}

	s := NewSplitter()
	actual := bytes.NewBuffer([]byte{})
	mock := &serviceMock{
		mockMkdirAll: func(path string, perm os.FileMode) error {
			assert.Equal(t, "out", path)
			return nil
		},
		mockCreateReader: func(fn string) (io.Reader, cleanupFunc, error) {
			assert.Equal(t, "in/file0", fn)
			return strings.NewReader(`line1
line2
line3
`), nop, nil
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
		Verbose:     Bool(true),
		Split:       Int(1),
		Parallelism: Int(1),
		Prefix:      String("out/file-"),
		Compress:    String("gzip"),
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
		mockCreateReader: func(fn string) (io.Reader, cleanupFunc, error) {
			assert.Equal(t, "in/file0", fn)
			return strings.NewReader(`line1
line2
line3
`), nop, nil
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

func TestOpenInput1(t *testing.T) {
	r, cleanup, err := openInput("/dev/null")
	assert.Nil(t, err)
	defer cleanup()
	_, ok := r.(*os.File)
	assert.True(t, ok)
}

func TestOpenInput2(t *testing.T) {
	r, cleanup, err := openInput("-")
	assert.Nil(t, err)
	defer cleanup()
	assert.True(t, os.Stdin == r)
}

func TestDecorateReaderNoCompression(t *testing.T) {
	r, cleanup, err := openInput("-")
	assert.Nil(t, err)
	defer cleanup()

	reader, cleanup, err := decorateReader("plain.txt", r)
	assert.Nil(t, err)
	defer cleanup()

	assert.True(t, r == reader)
}

func TestDecorateReaderGzip(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	w := gzip.NewWriter(buf)
	defer w.Close()
	w.Write([]byte("aaa"))
	w.Close()

	r := bytes.NewReader(buf.Bytes())

	reader, cleanup, err := decorateReader("path/to/some.tsv.gz", r)
	assert.Nil(t, err)
	defer cleanup()

	_, ok := reader.(*gzip.Reader)
	assert.True(t, ok)

	content, err := ioutil.ReadAll(reader)
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
