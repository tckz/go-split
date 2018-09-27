package split

func nop() {
	// nothing to do
}

func Int(v int) *int {
	return &v
}

func Bool(v bool) *bool {
	return &v
}

func String(v string) *string {
	return &v
}
