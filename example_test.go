package mgojq

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func Example() {
	// get jobs collection
	coll := Wrap(db.C("jobs"))

	// ensure indexes
	err := coll.EnsureIndexes()
	if err != nil {
		panic(err)
	}

	// create a worker pool
	pool := NewPool(1, 100*time.Millisecond, 1*time.Hour)

	// register worker
	pool.Register("Adder", func(c *Collection, j *Job, q <-chan struct{}) error {
		r := j.Params["a"].(int) + j.Params["b"].(int)
		c.Complete(j.ID, bson.M{"r": r})
		return nil
	})

	// start pool
	pool.Start(coll)
	defer pool.Close()

	// add job
	id, err := coll.Enqueue("Adder", bson.M{"a": 10, "b": 5}, 0)
	if err != nil {
		panic(err)
	}

	// wait some time
	time.Sleep(200 * time.Millisecond)

	// get job
	job, err := coll.Fetch(id)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s: %d\n", job.Status, job.Result["r"].(int))

	// Output:
	// completed: 15
}
