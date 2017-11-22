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

	jqc.Enqueue("foo", nil, 0)
	jqc.Enqueue("foo", nil, 0)
	jqc.Enqueue("foo", nil, 0)

	time.Sleep(10 * time.Millisecond)
	pool.Close()
	assert.NoError(t, pool.Wait())

	assert.Equal(t, 3, counter)
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
