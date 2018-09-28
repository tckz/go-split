package split

type cleanupFunc func()

type cleanups struct {
	funcs []cleanupFunc
}

func (c *cleanups) add(f cleanupFunc) {
	c.funcs = append(c.funcs, f)
}

func (c *cleanups) do() {
	for _, f := range c.funcs {
		defer f()
	}
}
