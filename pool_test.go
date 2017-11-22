package mgojq

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	dbc := db.C("test-pool")
	jqc := Wrap(dbc)

	counter := 0

	pool := NewPool(1, 0)
	pool.Register("foo", func(c *Collection, j *Job, quit <-chan struct{}) error {
		counter++
		c.Complete(j.ID, nil)
		return nil
	})

	pool.Start(jqc)

	for i := 0; i < 3; i++ {
		jqc.Enqueue("foo", nil, 0)
	}

	time.Sleep(10 * time.Millisecond)
	pool.Close()
	assert.NoError(t, pool.Wait())

	assert.Equal(t, 3, counter)
}

func TestPoolParallel(t *testing.T) {
	dbc := db.C("test-pool-parallel")
	jqc := Wrap(dbc)

	counter := 0

	pool := NewPool(10, 0)
	pool.Register("foo", func(c *Collection, j *Job, quit <-chan struct{}) error {
		time.Sleep(10 * time.Millisecond)
		counter++
		c.Complete(j.ID, nil)
		return nil
	})

	pool.Start(jqc)

	for i := 0; i < 10; i++ {
		jqc.Enqueue("foo", nil, 0)
	}

	time.Sleep(15 * time.Millisecond)
	pool.Close()
	assert.NoError(t, pool.Wait())

	assert.Equal(t, 10, counter)
}

func TestPoolWait(t *testing.T) {
	dbc := db.C("test-pool-wait")
	jqc := Wrap(dbc)

	counter := 0

	pool := NewPool(1, 100*time.Millisecond)
	pool.Register("foo", func(c *Collection, j *Job, quit <-chan struct{}) error {
		counter++
		c.Complete(j.ID, nil)
		return nil
	})

	pool.Start(jqc)

	time.Sleep(50 * time.Millisecond)
	jqc.Enqueue("foo", nil, 0)
	time.Sleep(50 * time.Millisecond)
	jqc.Enqueue("foo", nil, 0)
	time.Sleep(115 * time.Millisecond)

	pool.Close()
	assert.NoError(t, pool.Wait())

	assert.Equal(t, 2, counter)
}

func TestPoolError(t *testing.T) {
	dbc := db.C("test-pool-error")
	jqc := Wrap(dbc)

	pool := NewPool(1, 0)
	pool.Register("foo", func(c *Collection, j *Job, quit <-chan struct{}) error {
		return errors.New("some error")
	})

	pool.Start(jqc)

	jqc.Enqueue("foo", nil, 0)

	assert.Equal(t, "some error", pool.Wait().Error())
}

func TestPoolStartError(t *testing.T) {
	dbc := db.C("test-pool-start-error")
	jqc := Wrap(dbc)

	pool := NewPool(1, 0)
	pool.Register("foo", func(c *Collection, j *Job, quit <-chan struct{}) error {
		return nil
	})

	pool.Start(jqc)
	defer pool.Close()

	assert.Panics(t, func() {
		pool.Start(nil)
	})
}
