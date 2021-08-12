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

	"github.com/dustin/go-humanize"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/errgroup"
)

var nop = func() {}

func decorateWriter(compression string, w io.Writer) (io.Writer, cleanupFunc, error) {
	ct, _ := getCompressionType(compression)
	switch ct {
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
	r           io.Reader
	description string
}

func (s *Splitter) Do(ctx context.Context, files []string, param Param) (retErr error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chLine := make(chan line, param.Parallelism)

	egWrite, err := s.ParallelWrite(ctx, cancel, chLine, param.Split, param)
	if err != nil {
		return fmt.Errorf("ParallelWrite: %w", err)
	}

	egScan, err := s.ParallelFileScan(ctx, cancel, files, param.Parallelism, param, chLine)
	if err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("ParallelFileScan: %w", err))
	}

	if err := egScan.Wait(); err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("ParallelFileScan.Wait: %w", err))
	}
	close(chLine)

	if err := egWrite.Wait(); err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("ParallelWrite.Wait: %w", err))
	}

	return retErr
}

func (s *Splitter) ParallelWrite(ctx context.Context, cancel func(), chIn <-chan line, parallelism int, param Param) (_ *errgroup.Group, retErr error) {
	defer func() {
		if retErr != nil {
			cancel()
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < parallelism; i++ {
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
				}
			}
		})
	}

	return eg, nil
}

func (s *Splitter) ParallelScan(ctx context.Context, cancel func(), chIn <-chan readTarget, parallelism int, param Param, chOut chan<- line) (_ *errgroup.Group, retErr error) {
	defer func() {
		if retErr != nil {
			cancel()
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < parallelism; i++ {
		eg.Go(func() (retErr error) {
			defer func() {
				if retErr != nil {
					cancel()
				}
			}()

			for tg := range chIn {
				lc := int64(0)
				scanner := bufio.NewScanner(tg.r)
				for scanner.Scan() {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case chOut <- line(scanner.Text()):
						lc++
						if param.Verbose && lc%10000 == 0 {
							fmt.Fprintf(s.stderr, "%s, line=%s\n", tg.description, humanize.Comma(lc))
						}
					}
				}
				if err := scanner.Err(); err != nil {
					return fmt.Errorf("Scan: %w", err)
				}
				if param.Verbose {
					fmt.Fprintf(s.stderr, "%s, total=%s\n", tg.description, humanize.Comma(lc))
				}
			}
			return nil
		})
	}

	return eg, nil
}

func (s *Splitter) ParallelFileScan(ctx context.Context, cancel func(), files []string, parallelism int, param Param, chOut chan<- line) (_ *errgroup.Group, retErr error) {
	cleanups := cleanups{}
	defer func() {
		if retErr != nil {
			defer cancel()
			cleanups.do()
		}
	}()

	chTarget := make(chan readTarget, parallelism)

	egScan, err := s.ParallelScan(ctx, cancel, chTarget, parallelism, param, chOut)
	if err != nil {
		return nil, fmt.Errorf("ParallelScan: %w", err)
	}

	for _, fn := range files {
		if param.Verbose {
			fmt.Fprintf(s.stderr, "%s\n", fn)
		}
		r, cleanup, err := s.svc.createReader(fn)
		if err != nil {
			return nil, fmt.Errorf("createReader: %w", err)
		}
		cleanups.add(cleanup)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case chTarget <- readTarget{
			r:           r,
			description: fn,
		}:
		}
	}
	close(chTarget)

	eg, _ := errgroup.WithContext(ctx)
	eg.Go(func() (retErr error) {
		defer cleanups.do()
		defer func() {
			if retErr != nil {
				cancel()
			}
		}()
		return egScan.Wait()
	})

	return eg, nil
}

type serviceImpl struct{}

func (s *serviceImpl) createReader(fn string) (_ io.Reader, _ cleanupFunc, retErr error) {
	cleanups := &cleanups{}
	defer func() {
		if retErr != nil {
			cleanups.do()
		}
	}()

	fp, cleanup1, err := openInput(fn)
	if err != nil {
		return nil, nil, fmt.Errorf("openInput: %w", err)
	}
	cleanups.add(func() { cleanup1() })

	r, cleanup2, err := decorateReader(fn, fp)
	if err != nil {
		return nil, nil, fmt.Errorf("decorateReader: %w", err)
	}
	cleanups.add(func() { cleanup2() })

	return r, func() { cleanups.do() }, nil
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
	cleanups.add(func() { fp.Close() })

	w, cleanup, err := decorateWriter(compress, fp)
	if err != nil {
		return nil, nil, fmt.Errorf("decorateWriter: %w", err)
	}
	cleanups.add(func() { cleanup() })

	return w, func() { cleanups.do() }, nil
}
