// Package mgojq is a wrapper for mgo that turns MongoDB into a job queue.
package mgojq

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	enqueued  = "enqueued"
	dequeued  = "dequeued"
	completed = "completed"
	failed    = "failed"
	cancelled = "cancelled"
)

// TODO: Add retrying.
// TODO: Add delaying.
// TODO: Add priorities.

// A Job as it is returned by Dequeue.
type Job struct {
	ID     bson.ObjectId `bson:"_id"`
	Name   string
	Params bson.M
}

// A Bulk represents an operation that can be used to enqueue multiple jobs at
// once. It is a wrapper around the mgo.Bulk type.
type Bulk struct {
	coll *Collection
	bulk *mgo.Bulk
}

// Enqueue will queue the insert in the bulk operation.
func (b *Bulk) Enqueue(name string, params bson.M) {
	b.bulk.Insert(b.coll.insertJob(name, params))
}

// TODO: Add methods to dequeue, complete, fail and cancel many jobs at once.

// Run will insert all queued insert operations.
func (b *Bulk) Run() error {
	_, err := b.bulk.Run()
	return err
}

// A Collection represents a job queue enabled collection. It is a wrapper
// around the mgo.Collection type.
type Collection struct {
	coll *mgo.Collection
}

// Wrap will take a mgo.Collection and return a Collection.
func Wrap(coll *mgo.Collection) *Collection {
	return &Collection{
		coll: coll,
	}
}

// Enqueue will immediately write the specified metrics to the collection.
func (c *Collection) Enqueue(name string, params bson.M) error {
	return c.coll.Insert(c.insertJob(name, params))
}

// Bulk will return a new bulk operation.
func (c *Collection) Bulk() *Bulk {
	// create new bulk operation
	bulk := c.coll.Bulk()
	bulk.Unordered()

	return &Bulk{coll: c, bulk: bulk}
}

func (c *Collection) insertJob(name string, params bson.M) bson.M {
	return bson.M{
		"name":   name,
		"params": params,
		"status": enqueued,
	}
}

// Dequeue will try to dequeue a job and return its id and params.
func (c *Collection) Dequeue(names ...string) (*Job, error) {
	// check names
	if len(names) == 0 {
		panic("at least one job name is required")
	}

	var job Job
	_, err := c.coll.Find(bson.M{
		"name": bson.M{
			"$in": names,
		},
		"status": bson.M{
			"$in": []string{enqueued, failed},
		},
	}).Apply(mgo.Change{
		Update: bson.M{
			"$set": bson.M{
				"status":  dequeued,
				"started": time.Now(),
			},
		},
		ReturnNew: true,
	}, &job)
	if err == mgo.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return &job, nil
}

// Complete will complete the specified job.
func (c *Collection) Complete(id bson.ObjectId, result bson.M) error {
	return c.coll.UpdateId(id, bson.M{
		"$set": bson.M{
			"status": completed,
			"result": result,
			"ended":  time.Now(),
		},
	})
}

// Fail will fail the specified job.
func (c *Collection) Fail(id bson.ObjectId, error string) error {
	return c.coll.UpdateId(id, bson.M{
		"$set": bson.M{
			"status": failed,
			"error":  error,
			"ended":  time.Now(),
		},
	})
}

// Cancel will cancel the specified job.
func (c *Collection) Cancel(id bson.ObjectId, reason string) error {
	return c.coll.UpdateId(id, bson.M{
		"$set": bson.M{
			"status": cancelled,
			"reason": reason,
			"ended":  time.Now(),
		},
	})
}

// EnsureIndexes will ensure that the necessary indexes have been created.
//
// Note: It is recommended to create custom indexes that support the exact
// nature of data and access patterns.
func (c *Collection) EnsureIndexes() error {
	// ensure name index
	err := c.coll.EnsureIndex(mgo.Index{
		Key:        []string{"name"},
		Background: true,
	})
	if err != nil {
		return err
	}

	// ensure status index
	err = c.coll.EnsureIndex(mgo.Index{
		Key:        []string{"status"},
		Background: true,
	})
	if err != nil {
		return err
	}

	return nil
}
