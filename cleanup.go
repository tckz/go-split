package split

import (
	"sync"

	"github.com/hashicorp/go-multierror"
)

type cleanupFunc func() error

type cleanups struct {
	funcs []cleanupFunc
	once  sync.Once
}

func (c *cleanups) add(f cleanupFunc) {
	c.funcs = append(c.funcs, f)
}

func (c *cleanups) do() (err error) {
	c.once.Do(func() {
		for i := len(c.funcs) - 1; i >= 0; i-- {
			err = multierror.Append(err, c.funcs[i]())
		}
	})
	return err
}
