package mgojq

import (
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/assert"
)

func TestCollectionEnqueue(t *testing.T) {
	dbc := db.C("test-coll-enqueue")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", bson.M{"bar": "baz"}, 0)
	assert.NoError(t, err)

	var data []bson.M
	err = dbc.Find(nil).Select(bson.M{"_id": 0}).All(&data)
	assert.NoError(t, err)

	assert.Equal(t, []bson.M{
		{
			"name": "foo",
			"params": bson.M{
				"bar": "baz",
			},
			"status":   "enqueued",
			"attempts": 0,
			"created":  setTime,
			"delayed":  setTime,
		},
	}, replaceTimeSlice(data))
}

func TestCollectionBulk(t *testing.T) {
	dbc := db.C("test-coll-bulk")
	jqc := Wrap(dbc)

	bulk := jqc.Bulk()
	bulk.Enqueue("foo1", bson.M{"bar": 1}, 0)
	bulk.Enqueue("foo2", bson.M{"bar": 2}, 0)
	bulk.Enqueue("foo3", bson.M{"bar": 3}, 0)

	err := bulk.Run()
	assert.NoError(t, err)

	var ids []bson.ObjectId
	err = dbc.Find(nil).Distinct("_id", &ids)
	assert.NoError(t, err)
	assert.Len(t, ids, 3)

	bulk = jqc.Bulk()
	bulk.Complete(ids[0], bson.M{"bar": "bar"})
	bulk.Fail(ids[1], "some error", 0)
	bulk.Cancel(ids[2], "some reason")

	err = bulk.Run()
	assert.NoError(t, err)

	var data []bson.M
	err = dbc.Find(nil).Select(bson.M{"_id": 0}).All(&data)
	assert.NoError(t, err)

	assert.Equal(t, []bson.M{
		{
			"name": "foo1",
			"params": bson.M{
				"bar": 1,
			},
			"status":   "completed",
			"attempts": 0,
			"created":  setTime,
			"delayed":  setTime,
			"ended":    setTime,
			"result":   bson.M{"bar": "bar"},
		},
		{
			"name": "foo2",
			"params": bson.M{
				"bar": 2,
			},
			"status":   "failed",
			"attempts": 0,
			"created":  setTime,
			"delayed":  setTime,
			"ended":    setTime,
			"error":    "some error",
		},
		{
			"name": "foo3",
			"params": bson.M{
				"bar": 3,
			},
			"status":   "cancelled",
			"attempts": 0,
			"created":  setTime,
			"delayed":  setTime,
			"ended":    setTime,
			"reason":   "some reason",
		},
	}, replaceTimeSlice(data))
}

func TestCollectionFetch(t *testing.T) {
	dbc := db.C("test-coll-fetch")
	jqc := Wrap(dbc)

	id, err := jqc.Enqueue("foo", bson.M{"bar": "baz"}, 0)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, 0)
	assert.NoError(t, err)

	err = jqc.Complete(id, bson.M{"bar": "baz"})
	assert.NoError(t, err)

	job, err = jqc.Fetch(id)
	assert.NoError(t, err)
	assert.Equal(t, &Job{
		ID:       id,
		Name:     "foo",
		Params:   bson.M{"bar": "baz"},
		Status:   StatusCompleted,
		Created:  setTime,
		Delayed:  setTime,
		Started:  setTime,
		Ended:    setTime,
		Result:   bson.M{"bar": "baz"},
		Attempts: 1,
	}, replaceTimeJob(job))
}

func TestCollectionDequeue(t *testing.T) {
	dbc := db.C("test-coll-dequeue")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", bson.M{"bar": "baz"}, 0)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.True(t, job.ID.Valid())
	assert.Equal(t, "foo", job.Name)
	assert.Equal(t, bson.M{"bar": "baz"}, job.Params)
	assert.Equal(t, 1, job.Attempts)

	job, err = jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.Nil(t, job)
}

func TestCollectionDequeueDelayed(t *testing.T) {
	dbc := db.C("test-coll-dequeue-delayed")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", bson.M{"bar": "baz"}, 100*time.Millisecond)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.Nil(t, job)

	time.Sleep(120 * time.Millisecond)

	job, err = jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.True(t, job.ID.Valid())
	assert.Equal(t, "foo", job.Name)
	assert.Equal(t, bson.M{"bar": "baz"}, job.Params)
	assert.Equal(t, 1, job.Attempts)

	job, err = jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.Nil(t, job)
}

func TestCollectionDequeueTimeout(t *testing.T) {
	dbc := db.C("test-coll-dequeue-timeout")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", nil, 0)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, 0)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = jqc.Dequeue([]string{"foo"}, 100*time.Millisecond)
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = jqc.Dequeue([]string{"foo"}, 0)
	assert.NoError(t, err)
	assert.NotNil(t, job)
}

func TestCollectionDequeueOldFirst(t *testing.T) {
	dbc := db.C("test-coll-dequeue-old-first")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", bson.M{"first": true}, 0)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	err = jqc.Fail(job.ID, "some error", 0)
	assert.NoError(t, err)

	_, err = jqc.Enqueue("foo", bson.M{"second": true}, 0)
	assert.NoError(t, err)

	job, err = jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"first": true}, job.Params)

	job, err = jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"second": true}, job.Params)

	job, err = jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.Nil(t, job)
}

func TestCollectionDequeueFailed(t *testing.T) {
	dbc := db.C("test-coll-dequeue-failed")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", bson.M{"bar": "baz"}, 0)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.True(t, job.ID.Valid())
	assert.Equal(t, "foo", job.Name)
	assert.Equal(t, bson.M{"bar": "baz"}, job.Params)
	assert.Equal(t, 1, job.Attempts)

	err = jqc.Fail(job.ID, "some error", 0)
	assert.NoError(t, err)

	job2, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.Equal(t, job.ID, job2.ID)
	assert.Equal(t, "foo", job2.Name)
	assert.Equal(t, bson.M{"bar": "baz"}, job2.Params)
	assert.Equal(t, 2, job2.Attempts)
}

func TestCollectionDequeueFailedDelayed(t *testing.T) {
	dbc := db.C("test-coll-dequeue-failed-delayed")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", bson.M{"bar": "baz"}, 0)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.True(t, job.ID.Valid())
	assert.Equal(t, "foo", job.Name)
	assert.Equal(t, bson.M{"bar": "baz"}, job.Params)
	assert.Equal(t, 1, job.Attempts)

	err = jqc.Fail(job.ID, "some error", 100*time.Millisecond)
	assert.NoError(t, err)

	job2, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.Nil(t, job2)

	time.Sleep(120 * time.Millisecond)

	job3, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.Equal(t, job.ID, job3.ID)
	assert.Equal(t, "foo", job3.Name)
	assert.Equal(t, bson.M{"bar": "baz"}, job3.Params)
	assert.Equal(t, 2, job3.Attempts)
}

func TestCollectionDequeuePanic(t *testing.T) {
	dbc := db.C("test-coll-dequeue-panic")
	jqc := Wrap(dbc)

	assert.Panics(t, func() {
		jqc.Dequeue(nil, 0)
	})
}

func TestCollectionComplete(t *testing.T) {
	dbc := db.C("test-coll-complete")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", bson.M{"bar": "baz"}, 0)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	err = jqc.Complete(job.ID, bson.M{"bar": "baz"})
	assert.NoError(t, err)

	var data bson.M
	err = dbc.FindId(job.ID).Select(bson.M{"_id": 0}).One(&data)
	assert.NoError(t, err)
	assert.Equal(t, bson.M{
		"name": "foo",
		"params": bson.M{
			"bar": "baz",
		},
		"status":   "completed",
		"created":  setTime,
		"delayed":  setTime,
		"attempts": 1,
		"started":  setTime,
		"result": bson.M{
			"bar": "baz",
		},
		"ended": setTime,
	}, replaceTimeMap(data))
}

func TestCollectionFail(t *testing.T) {
	dbc := db.C("test-coll-fail")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", bson.M{"bar": "baz"}, 0)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	err = jqc.Fail(job.ID, "some error", 0)
	assert.NoError(t, err)

	var data bson.M
	err = dbc.FindId(job.ID).Select(bson.M{"_id": 0}).One(&data)
	assert.NoError(t, err)
	assert.Equal(t, bson.M{
		"name": "foo",
		"params": bson.M{
			"bar": "baz",
		},
		"status":   "failed",
		"created":  setTime,
		"delayed":  setTime,
		"attempts": 1,
		"started":  setTime,
		"error":    "some error",
		"ended":    setTime,
	}, replaceTimeMap(data))
}

func TestCollectionCancel(t *testing.T) {
	dbc := db.C("test-coll-cancel")
	jqc := Wrap(dbc)

	_, err := jqc.Enqueue("foo", bson.M{"bar": "baz"}, 0)
	assert.NoError(t, err)

	job, err := jqc.Dequeue([]string{"foo"}, time.Hour)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	err = jqc.Cancel(job.ID, "some reason")
	assert.NoError(t, err)

	var data bson.M
	err = dbc.FindId(job.ID).Select(bson.M{"_id": 0}).One(&data)
	assert.NoError(t, err)
	assert.Equal(t, bson.M{
		"name": "foo",
		"params": bson.M{
			"bar": "baz",
		},
		"status":   "cancelled",
		"created":  setTime,
		"delayed":  setTime,
		"attempts": 1,
		"started":  setTime,
		"reason":   "some reason",
		"ended":    setTime,
	}, replaceTimeMap(data))
}

func TestCollectionEnsureIndexes(t *testing.T) {
	dbc := db.C("test-coll-ensure-indexes")
	jqc := Wrap(dbc)

	assert.NoError(t, jqc.EnsureIndexes(7 * 24 * time.Hour))
	assert.NoError(t, jqc.EnsureIndexes(7 * 24 * time.Hour))
}
