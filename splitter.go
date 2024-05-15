package split

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/dustin/go-humanize"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/errgroup"
)

var nop = func() error { return nil }

func decorateWriter(compression string, w io.Writer) (io.Writer, cleanupFunc, error) {
	ct, _ := getCompressionType(compression)
	switch ct {
	case CompressionNone:
		return w, nop, nil
	case CompressionGzip:
		gzw := gzip.NewWriter(w)
		return gzw, func() error { return gzw.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown compression type: %s", compression)
	}
}

func decorateReader(fn string, r io.Reader) (io.ReadCloser, error) {
	if strings.HasSuffix(fn, ".gz") {
		if gzr, err := gzip.NewReader(r); err != nil {
			return nil, err
		} else {
			return gzr, nil
		}
	} else if strings.HasSuffix(fn, ".bz2") {
		return io.NopCloser(bzip2.NewReader(r)), nil
	}
	return io.NopCloser(r), nil
}

type service interface {
	createWriter(fn string, compress string) (io.Writer, cleanupFunc, error)
	createReader(fn string) (io.ReadCloser, error)
	mkdirAll(path string, perm os.FileMode) error
}

type Param struct {
	Verbose     bool
	Split       int
	Parallelism int
	Prefix      string
	Compress    string
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

type line string

type readTarget struct {
	path string
}

func (s *Splitter) Do(ctx context.Context, files []string, param Param) (retErr error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chLine := make(chan line, param.Parallelism)

	futureWrite, err := s.write(ctx, cancel, chLine, param.Split, param)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}

	chTarget := make(chan readTarget, param.Parallelism)

	futureScan, err := s.scan(ctx, cancel, chTarget, param.Parallelism, param, chLine)
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}

	futureFile, err := s.file(ctx, cancel, files, param, chTarget)
	if err != nil {
		return fmt.Errorf("file: %w", err)
	}

	if _, err := futureFile.Get(); err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("file.Wait: %w", err))
	}
	close(chTarget)

	if _, err := futureScan.Get(); err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("scan.Wait: %w", err))
	}
	close(chLine)

	if _, err := futureWrite.Get(); err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("write.Get: %w", err))
	}

	return retErr
}

type Future[T any] interface {
	Get() (T, error)
}

var _ Future[int64] = (*future[int64])(nil)

type future[T any] struct {
	eg     *errgroup.Group
	result T
}

func (f *future[T]) Get() (T, error) {
	err := f.eg.Wait()
	return f.result, err
}

func (s *Splitter) write(ctx context.Context, cancel func(), chIn <-chan line, parallelism int, param Param) (_ Future[int64], retErr error) {
	defer func() {
		if retErr != nil {
			cancel()
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)
	ret := future[int64]{
		eg:     eg,
		result: 0,
	}
	for i := range parallelism {
		_, suffix := getCompressionType(param.Compress)

		fn := fmt.Sprintf("%s%03d%s", param.Prefix, i, suffix)
		dir := path.Dir(fn)
		if err := s.svc.mkdirAll(dir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("mkdirAll: %w", err)
		}

		eg.Go(func() (retErr error) {
			defer func() {
				if retErr != nil {
					cancel()
				}
			}()

			w, cleanup, err := s.svc.createWriter(fn, param.Compress)
			if err != nil {
				return fmt.Errorf("createWriter: %w", err)
			}
			defer cleanup()

			lf := []byte("\n")
			lc := int64(0)
			defer func() {
				atomic.AddInt64(&ret.result, lc)
			}()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case line, ok := <-chIn:
					if !ok {
						return nil
					}
					w.Write([]byte(line))
					w.Write(lf)
					lc++
				}
			}
		})
	}

	return &ret, nil
}

func (s *Splitter) scan(ctx context.Context, cancel func(), chIn <-chan readTarget, parallelism int, param Param, chOut chan<- line) (_ Future[lineCount], retErr error) {
	defer func() {
		if retErr != nil {
			cancel()
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)
	ret := future[lineCount]{
		eg:     eg,
		result: 0,
	}
	for range parallelism {
		eg.Go(func() (retErr error) {
			defer func() {
				if retErr != nil {
					cancel()
				}
			}()

			for tg := range chIn {
				err := func() error {
					r, err := s.svc.createReader(tg.path)
					if err != nil {
						return fmt.Errorf("createReader: %w", err)
					}

					defer r.Close()

					lc := int64(0)
					scanner := bufio.NewScanner(r)
					for scanner.Scan() {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case chOut <- line(scanner.Text()):
							lc++
							if param.Verbose && lc%10000 == 0 {
								fmt.Fprintf(s.stderr, "%s, line=%s\n", tg.path, humanize.Comma(lc))
							}
						}
					}
					if err := scanner.Err(); err != nil {
						return fmt.Errorf("Scan: %w", err)
					}
					if param.Verbose {
						fmt.Fprintf(s.stderr, "%s, total=%s\n", tg.path, humanize.Comma(lc))
					}
					atomic.AddInt64((*int64)(&ret.result), lc)
					return nil
				}()
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	return &ret, nil
}

type lineCount int64
type fileCount int64

func (s *Splitter) file(ctx context.Context, cancel func(), files []string, param Param, chOut chan<- readTarget) (_ Future[fileCount], retErr error) {
	defer func() {
		if retErr != nil {
			defer cancel()
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)
	ret := future[fileCount]{
		eg:     eg,
		result: 0,
	}
	eg.Go(func() (retErr error) {
		defer func() {
			if retErr != nil {
				cancel()
			}
		}()

		for _, fn := range files {
			if param.Verbose {
				fmt.Fprintf(s.stderr, "%s\n", fn)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case chOut <- readTarget{
				path: fn,
			}:
				atomic.AddInt64((*int64)(&ret.result), 1)
			}
		}

		return nil
	})

	return &ret, nil
}

type serviceImpl struct{}

var _ io.ReadCloser = (*readCleanupCloser)(nil)

type readCleanupCloser struct {
	io.Reader
	cleanups *cleanups
}

func (c *readCleanupCloser) Close() error {
	return c.cleanups.do()
}

func (s *serviceImpl) createReader(fn string) (_ io.ReadCloser, retErr error) {
	cleanups := &cleanups{}
	defer func() {
		if retErr != nil {
			_ = cleanups.do()
		}
	}()

	fp, err := os.Open(fn)
	if err != nil {
		return nil, fmt.Errorf("os.Open: %w", err)
	}
	cleanups.add(func() error { return fp.Close() })

	r, err := decorateReader(fn, fp)
	if err != nil {
		return nil, fmt.Errorf("decorateReader: %w", err)
	}
	cleanups.add(func() error { return r.Close() })

	ret := &readCleanupCloser{
		Reader:   r,
		cleanups: cleanups,
	}
	return ret, nil
}

func (s *serviceImpl) mkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (s *serviceImpl) createWriter(fn string, compress string) (_ io.Writer, _ cleanupFunc, retErr error) {
	cleanups := &cleanups{}
	defer func() {
		if retErr != nil {
			cleanups.do()
		}
	}()

	fp, err := os.Create(fn)
	if err != nil {
		return nil, nil, err
	}
	cleanups.add(func() error { return fp.Close() })

	w, cleanup, err := decorateWriter(compress, fp)
	if err != nil {
		return nil, nil, fmt.Errorf("decorateWriter: %w", err)
	}
	cleanups.add(func() error { return cleanup() })

	return w, func() error { return cleanups.do() }, nil
}
