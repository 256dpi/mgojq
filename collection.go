// Package mgojq is a wrapper for mgo that turns MongoDB into a job queue.
package mgojq

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// The available job statuses.
const (
	StatusEnqueued  = "enqueued"
	StatusDequeued  = "dequeued"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// A Job as it is returned by Dequeue.
type Job struct {
	// The unique id of the job.
	ID bson.ObjectId `bson:"_id"`

	// The name of the job.
	Name string

	// The params that have been supplied on creation.
	Params bson.M

	// The current status of the job.
	Status string

	// The time when the job was created.
	Created time.Time

	// The time until the job is delayed for execution.
	Delayed time.Time `bson:",omitempty"`

	// The time when the job was the last time dequeued.
	Started time.Time `bson:",omitempty"`

	// The time when the job was ended (completed, failed or cancelled).
	Ended time.Time `bson:",omitempty"`

	// Attempts can be used to determine if a job should be cancelled after too
	// many attempts.
	Attempts int

	// The supplied result submitted during completion.
	Result bson.M `bson:",omitempty"`

	// The error from the last failed attempt.
	Error string `bson:",omitempty"`

	// The reason that has been submitted when job was cancelled.
	Reason string `bson:",omitempty"`
}

// A Bulk represents an operation that can be used to enqueue multiple jobs at
// once. It is a wrapper around the mgo.Bulk type.
type Bulk struct {
	coll *Collection
	bulk *mgo.Bulk
}

// Enqueue will queue the insert in the bulk operation. The returned id is only
// valid if the bulk operation run successfully,.
func (b *Bulk) Enqueue(name string, params bson.M, delay time.Duration) bson.ObjectId {
	id, doc := b.coll.insertJob(name, params, delay)
	b.bulk.Insert(doc)
	return id
}

// Complete will queue the complete in the bulk operation.
func (b *Bulk) Complete(id bson.ObjectId, result bson.M) {
	b.bulk.Update(bson.M{"_id": id}, b.coll.completeJob(result))
}

// Fail will queue the fail in the bulk operation.
func (b *Bulk) Fail(id bson.ObjectId, error string, delay time.Duration) {
	b.bulk.Update(bson.M{"_id": id}, b.coll.failJob(error, delay))
}

// Cancel will queue the cancel in the bulk operation.
func (b *Bulk) Cancel(id bson.ObjectId, reason string) {
	b.bulk.Update(bson.M{"_id": id}, b.coll.cancelJob(reason))
}

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

// Enqueue will enqueue a job using the specified name and params. If a delay
// is specified the job will not dequeued until the specified time has passed.
// If not error is returned the returned job id is valid.
func (c *Collection) Enqueue(name string, params bson.M, delay time.Duration) (bson.ObjectId, error) {
	id, doc := c.insertJob(name, params, delay)
	return id, c.coll.Insert(doc)
}

func (c *Collection) insertJob(name string, params bson.M, delay time.Duration) (bson.ObjectId, *Job) {
	id := bson.NewObjectId()

	return id, &Job{
		ID:      id,
		Name:    name,
		Params:  params,
		Status:  StatusEnqueued,
		Created: time.Now(),
		Delayed: time.Now().Add(delay),
	}
}

// Bulk will return a new bulk operation.
func (c *Collection) Bulk() *Bulk {
	// create new bulk operation
	bulk := c.coll.Bulk()
	bulk.Unordered()

	return &Bulk{coll: c, bulk: bulk}
}

// Dequeue will try to dequeue a job.
func (c *Collection) Dequeue(names []string, timeout time.Duration) (*Job, error) {
	// check names
	if len(names) == 0 {
		panic("at least one job name is required")
	}

	var job Job
	_, err := c.coll.Find(bson.M{
		"name": bson.M{
			"$in": names,
		},
		"$or": []bson.M{
			{
				"status": bson.M{
					"$in": []string{StatusEnqueued, StatusFailed},
				},
				"delayed": bson.M{
					"$lte": time.Now(),
				},
			},
			{
				"status": StatusDequeued,
				"started": bson.M{
					"$lte": time.Now().Add(-timeout),
				},
			},
		},
	}).Sort("_id").Apply(mgo.Change{
		Update: bson.M{
			"$set": bson.M{
				"status":  StatusDequeued,
				"started": time.Now(),
			},
			"$inc": bson.M{
				"attempts": 1,
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

// Fetch will load the job with the specified id.
func (c *Collection) Fetch(id bson.ObjectId) (*Job, error) {
	var job Job
	return &job, c.coll.FindId(id).One(&job)
}

// Complete will complete the specified job and set the specified result.
func (c *Collection) Complete(id bson.ObjectId, result bson.M) error {
	return c.coll.UpdateId(id, c.completeJob(result))
}

func (c *Collection) completeJob(result bson.M) bson.M {
	return bson.M{
		"$set": bson.M{
			"status": StatusCompleted,
			"result": result,
			"ended":  time.Now(),
		},
	}
}

// Fail will fail the specified job with the specified error. Delay can be set
// enforce a delay until the job can be dequeued again.
func (c *Collection) Fail(id bson.ObjectId, error string, delay time.Duration) error {
	return c.coll.UpdateId(id, c.failJob(error, delay))
}

func (c *Collection) failJob(error string, delay time.Duration) bson.M {
	return bson.M{
		"$set": bson.M{
			"status":  StatusFailed,
			"error":   error,
			"ended":   time.Now(),
			"delayed": time.Now().Add(delay),
		},
	}
}

// Cancel will cancel the specified job with the specified reason.
func (c *Collection) Cancel(id bson.ObjectId, reason string) error {
	return c.coll.UpdateId(id, c.cancelJob(reason))
}

func (c *Collection) cancelJob(reason string) bson.M {
	return bson.M{
		"$set": bson.M{
			"status": StatusCancelled,
			"reason": reason,
			"ended":  time.Now(),
		},
	}
}

// EnsureIndexes will ensure that the necessary indexes have been created. If
// removeAfter is specified, jobs are automatically removed when their ended
// timestamp falls behind the specified duration. Warning: this also applies
// to failed jobs!
//
// Note: It is recommended to create custom indexes that support the exact
// nature of data and access patterns.
func (c *Collection) EnsureIndexes(removeAfter time.Duration) error {
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

	// ensure ended index
	err = c.coll.EnsureIndex(mgo.Index{
		Key:         []string{"ended"},
		ExpireAfter: removeAfter,
		Background:  true,
	})
	if err != nil {
		return err
	}

	return nil
}
