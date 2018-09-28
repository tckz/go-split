package split

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCleanup0(t *testing.T) {
	cleanups := &cleanups{}

	called := []string{}
	cleanups.do()

	assert.Equal(t, []string{}, called)
}

func TestCleanup1(t *testing.T) {
	cleanups := &cleanups{}

	called := []string{}
	cleanups.add(func() {
		called = append(called, "call1")
	})

	cleanups.do()

	assert.Equal(t, []string{"call1"}, called)
}

func TestCleanup3(t *testing.T) {
	cleanups := &cleanups{}

	called := []string{}

	cleanups.add(func() {
		called = append(called, "call1")
	})

	cleanups.add(func() {
		called = append(called, "call2")
	})

	cleanups.add(func() {
		called = append(called, "call3")
	})

	cleanups.do()

	assert.Equal(t, []string{
		"call3",
		"call2",
		"call1",
	}, called)
}
