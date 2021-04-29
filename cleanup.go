package split

import "sync"

type cleanupFunc func()

type cleanups struct {
	funcs []cleanupFunc
	once  sync.Once
}

func (c *cleanups) add(f cleanupFunc) {
	c.funcs = append(c.funcs, f)
}

func (c *cleanups) do() {
	c.once.Do(func() {
		for _, f := range c.funcs {
			defer f()
		}
	})
}
