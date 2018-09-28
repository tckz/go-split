package split

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
)

func decorateWriter(compression string, w io.Writer) (io.Writer, cleanupFunc, error) {
	switch getCompressionType(compression) {
	case CompressionNone:
		return w, nop, nil
	case CompressionGzip:
		gzw := gzip.NewWriter(w)
		return gzw, func() { gzw.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown compression type: %s", compression)
	}
}

func decorateReader(fn string, r io.Reader) (io.Reader, cleanupFunc, error) {
	if strings.HasSuffix(fn, ".gz") {
		if gzr, err := gzip.NewReader(r); err != nil {
			return nil, nil, err
		} else {
			return gzr, func() { gzr.Close() }, nil
		}
	} else if strings.HasSuffix(fn, ".bz2") {
		return bzip2.NewReader(r), nop, nil
	}
	return r, nop, nil
}

func openInput(fn string) (io.Reader, cleanupFunc, error) {
	if fn == "-" {
		return os.Stdin, nop, nil
	}

	if r, err := os.Open(fn); err != nil {
		return nil, nil, err
	} else {
		return r, func() { r.Close() }, nil
	}

}

type service interface {
	createWriter(fn string, compress string) (io.Writer, cleanupFunc, error)
	createReader(fn string) (io.Reader, cleanupFunc, error)
	mkdirAll(path string, perm os.FileMode) error
}

type Param struct {
	Verbose     *bool
	Split       *int
	Parallelism *int
	Prefix      *string
	Compress    *string
}

type Splitter struct {
	stderr io.Writer
	svc    service
}

func NewSplitter() *Splitter {
	return &Splitter{
		stderr: os.Stderr,
		svc:    &serviceImpl{},
	}
}

func (s *Splitter) Do(files []string, param Param) {

	// goroutines for output
	chLine := make(chan string, *param.Split)
	var wgWrite sync.WaitGroup
	for i := 0; i < *param.Split; i++ {
		wgWrite.Add(1)

		ct := getCompressionType(*param.Compress)
		var suffix = ""
		if ct == CompressionGzip {
			suffix = ".gz"
		}

		fn := fmt.Sprintf("%s%03d%s", *param.Prefix, i, suffix)
		dir := path.Dir(fn)
		if err := s.svc.mkdirAll(dir, os.ModePerm); err != nil {
			log.Fatalf("*** Failed to mkdirAll: %v", err)
		}

		go func() {
			defer wgWrite.Done()

			w, cleanup, err := s.svc.createWriter(fn, *param.Compress)
			if err != nil {
				log.Fatalf("*** Failed to createWriter: %v", err)
			}
			defer cleanup()

			lf := []byte("\n")
			for line := range chLine {
				w.Write([]byte(line))
				w.Write(lf)
			}
		}()
	}

	// goroutines for input
	chFile := make(chan string, *param.Parallelism)
	var wgRead sync.WaitGroup
	for i := 0; i < *param.Parallelism; i++ {
		wgRead.Add(1)
		go func() {
			defer wgRead.Done()

			for fn := range chFile {
				if *param.Verbose {
					fmt.Fprintf(s.stderr, "%s\n", fn)
				}

				func() {
					r, cleanup, err := s.svc.createReader(fn)
					if err != nil {
						log.Fatalf("*** Failed to OpenReader: %v", err)
					}
					defer cleanup()

					lc := int64(0)
					scanner := bufio.NewScanner(r)
					for scanner.Scan() {
						chLine <- scanner.Text()
						lc++
						if *param.Verbose && lc%10000 == 0 {
							fmt.Fprintf(s.stderr, "%s, line=%s\n", fn, humanize.Comma(lc))
						}
					}
					if err := scanner.Err(); err != nil {
						log.Fatalf("*** Failed to Scan: %v", err)
					}
					if *param.Verbose {
						fmt.Fprintf(s.stderr, "%s, total=%s\n", fn, humanize.Comma(lc))
					}
				}()
			}
		}()
	}

	for _, arg := range files {
		chFile <- arg
	}
	close(chFile)
	wgRead.Wait()

	close(chLine)
	wgWrite.Wait()
}

type serviceImpl struct{}

func (s *serviceImpl) createReader(fn string) (io.Reader, cleanupFunc, error) {
	cleanups := &cleanups{}

	fp, cleanup1, err := openInput(fn)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "*** Failed to openInput")
	}
	cleanups.add(func() { cleanup1() })

	r, cleanup2, err := decorateReader(fn, fp)
	if err != nil {
		defer cleanups.do()
		return nil, nil, errors.Wrapf(err, "*** Failed to decorateReader")
	}
	cleanups.add(func() { cleanup2() })

	return r, func() { cleanups.do() }, nil
}

func (s *serviceImpl) mkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (s *serviceImpl) createWriter(fn string, compress string) (io.Writer, cleanupFunc, error) {

	cleanups := &cleanups{}

	fp, err := os.Create(fn)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "*** Failed to Create")
	}
	cleanups.add(func() { fp.Close() })

	w, cleanup, err := decorateWriter(compress, fp)
	if err != nil {
		defer cleanups.do()
		return nil, nil, errors.Wrapf(err, "*** Failed to decorateWriter")
	}
	cleanups.add(func() { cleanup() })

	return w, func() { cleanups.do() }, nil
}
