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
	"golang.org/x/sync/errgroup"
)

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

type line string

type readTarget struct {
	r           io.Reader
	description string
}

func (s *Splitter) Do(ctx context.Context, files []string, param Param) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chLine := make(chan line, *param.Parallelism)

	egWrite, err := s.ParallelWrite(ctx, chLine, *param.Split, param)
	if err != nil {
		return fmt.Errorf("ParallelWrite: %w", err)
	}

	egScan, err := s.ParallelFileScan(ctx, files, *param.Parallelism, param, chLine)
	if err != nil {
		return fmt.Errorf("ParallelFileScan: %w", err)
	}

	if err := egScan.Wait(); err != nil {
		return fmt.Errorf("ParallelFileScan.Wait: %w", err)
	}
	close(chLine)

	if err := egWrite.Wait(); err != nil {
		return fmt.Errorf("ParallelWrite.Wait: %w", err)
	}

	return nil
}

func (s *Splitter) ParallelWrite(ctx context.Context, chIn <-chan line, parallelism int, param Param) (*errgroup.Group, error) {

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < parallelism; i++ {
		_, suffix := getCompressionType(*param.Compress)

		fn := fmt.Sprintf("%s%03d%s", *param.Prefix, i, suffix)
		dir := path.Dir(fn)
		if err := s.svc.mkdirAll(dir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("mkdirAll: %w", err)
		}

		eg.Go(func() error {
			w, cleanup, err := s.svc.createWriter(fn, *param.Compress)
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

func (s *Splitter) ParallelScan(ctx context.Context, chIn <-chan readTarget, parallelism int, param Param, chOut chan<- line) (*errgroup.Group, error) {
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < parallelism; i++ {
		eg.Go(func() error {
			for tg := range chIn {
				lc := int64(0)
				scanner := bufio.NewScanner(tg.r)
				for scanner.Scan() {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					chOut <- line(scanner.Text())
					lc++
					if *param.Verbose && lc%10000 == 0 {
						fmt.Fprintf(s.stderr, "%s, line=%s\n", tg.description, humanize.Comma(lc))
					}
				}
				if err := scanner.Err(); err != nil {
					return fmt.Errorf("Scan: %w", err)
				}
				if *param.Verbose {
					fmt.Fprintf(s.stderr, "%s, total=%s\n", tg.description, humanize.Comma(lc))
				}
			}
			return nil
		})
	}

	return eg, nil
}

func (s *Splitter) ParallelFileScan(ctx context.Context, files []string, parallelism int, param Param, chOut chan<- line) (*errgroup.Group, error) {
	chTarget := make(chan readTarget, parallelism)

	egScan, err := s.ParallelScan(ctx, chTarget, parallelism, param, chOut)
	if err != nil {
		return nil, fmt.Errorf("ParallelScan: %w", err)
	}

	cleanups := cleanups{}
	for _, fn := range files {
		if *param.Verbose {
			fmt.Fprintf(s.stderr, "%s\n", fn)
		}
		r, cleanup, err := s.svc.createReader(fn)
		if err != nil {
			cleanups.do()
			return nil, fmt.Errorf("createReader: %w", err)
		}
		cleanups.add(cleanup)

		chTarget <- readTarget{
			r:           r,
			description: fn,
		}
	}
	close(chTarget)

	eg, _ := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer cleanups.do()
		return egScan.Wait()
	})

	return eg, nil
}

type serviceImpl struct{}

func (s *serviceImpl) createReader(fn string) (io.Reader, cleanupFunc, error) {
	cleanups := &cleanups{}

	fp, cleanup1, err := openInput(fn)
	if err != nil {
		return nil, nil, fmt.Errorf("openInput: %w", err)
	}
	cleanups.add(func() { cleanup1() })

	r, cleanup2, err := decorateReader(fn, fp)
	if err != nil {
		defer cleanups.do()
		return nil, nil, fmt.Errorf("decorateReader: %w", err)
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
		return nil, nil, err
	}
	cleanups.add(func() { fp.Close() })

	w, cleanup, err := decorateWriter(compress, fp)
	if err != nil {
		defer cleanups.do()
		return nil, nil, fmt.Errorf("decorateWriter: %w", err)
	}
	cleanups.add(func() { cleanup() })

	return w, func() { cleanups.do() }, nil
}
