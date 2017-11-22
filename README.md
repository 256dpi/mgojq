# mgojq
    
[![Build Status](https://travis-ci.org/256dpi/mgojq.svg?branch=master)](https://travis-ci.org/256dpi/mgojq)
[![Coverage Status](https://coveralls.io/repos/github/256dpi/mgojq/badge.svg?branch=master)](https://coveralls.io/github/256dpi/mgojq?branch=master)
[![GoDoc](https://godoc.org/github.com/256dpi/mgojq?status.svg)](http://godoc.org/github.com/256dpi/mgojq)
[![Release](https://img.shields.io/github/release/256dpi/mgojq.svg)](https://github.com/256dpi/mgojq/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/256dpi/mgojq)](https://goreportcard.com/report/github.com/256dpi/mgojq)

**A wrapper for [mgo](https://github.com/go-mgo/mgo) that turns MongoDB into a job queue.**

## Example

```go
// get jobs collection
coll := Wrap(db.C("jobs"))

// ensure indexes
err := coll.EnsureIndexes(7 * 24 * time.Hour)
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
```
