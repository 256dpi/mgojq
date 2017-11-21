package mgojq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestCollectionEnqueue(t *testing.T) {
	dbc := db.C("test-coll-enqueue")
	jqc := Wrap(dbc)

	err := jqc.Enqueue("foo", bson.M{"bar": "baz"})
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
		},
	}, data)
}

func TestCollectionBulkEnqueue(t *testing.T) {
	dbc := db.C("test-coll-bulk-enqueue")
	jqc := Wrap(dbc)
	bulk := jqc.Bulk()

	for i := 0; i < 2; i++ {
		bulk.Enqueue("foo", bson.M{"bar": i})
	}

	err := bulk.Run()
	assert.NoError(t, err)

	var data []bson.M
	err = dbc.Find(nil).Select(bson.M{"_id": 0}).All(&data)
	assert.NoError(t, err)

	assert.Equal(t, []bson.M{
		{
			"name": "foo",
			"params": bson.M{
				"bar": 0,
			},
			"status":   "enqueued",
			"attempts": 0,
		},
		{
			"name": "foo",
			"params": bson.M{
				"bar": 1,
			},
			"status":   "enqueued",
			"attempts": 0,
		},
	}, data)
}

func TestCollectionDequeue(t *testing.T) {
	dbc := db.C("test-coll-dequeue")
	jqc := Wrap(dbc)

	err := jqc.Enqueue("foo", bson.M{"bar": "baz"})
	assert.NoError(t, err)

	job, err := jqc.Dequeue("foo")
	assert.NoError(t, err)
	assert.True(t, job.ID.Valid())
	assert.Equal(t, "foo", job.Name)
	assert.Equal(t, bson.M{"bar": "baz"}, job.Params)
	assert.Equal(t, 1, job.Attempts)

	job, err = jqc.Dequeue("foo")
	assert.NoError(t, err)
	assert.Nil(t, job)
}

func TestCollectionDequeueFailed(t *testing.T) {
	dbc := db.C("test-coll-dequeue-failed")
	jqc := Wrap(dbc)

	err := jqc.Enqueue("foo", bson.M{"bar": "baz"})
	assert.NoError(t, err)

	job, err := jqc.Dequeue("foo")
	assert.NoError(t, err)
	assert.True(t, job.ID.Valid())
	assert.Equal(t, "foo", job.Name)
	assert.Equal(t, bson.M{"bar": "baz"}, job.Params)
	assert.Equal(t, 1, job.Attempts)

	err = jqc.Fail(job.ID, "some error")
	assert.NoError(t, err)

	job2, err := jqc.Dequeue("foo")
	assert.NoError(t, err)
	assert.Equal(t, job.ID, job2.ID)
	assert.Equal(t, "foo", job2.Name)
	assert.Equal(t, bson.M{"bar": "baz"}, job2.Params)
	assert.Equal(t, 2, job2.Attempts)
}

func TestCollectionDequeuePanic(t *testing.T) {
	dbc := db.C("test-coll-dequeue")
	jqc := Wrap(dbc)

	assert.Panics(t, func() {
		jqc.Dequeue()
	})
}

func TestCollectionComplete(t *testing.T) {
	dbc := db.C("test-coll-complete")
	jqc := Wrap(dbc)

	err := jqc.Enqueue("foo", bson.M{"bar": "baz"})
	assert.NoError(t, err)

	job, err := jqc.Dequeue("foo")
	assert.NoError(t, err)
	assert.NotNil(t, job)

	err = jqc.Complete(job.ID, bson.M{"bar": "baz"})
	assert.NoError(t, err)

	var data bson.M
	err = dbc.FindId(job.ID).Select(bson.M{"_id": 0}).One(&data)
	assert.NoError(t, err)
	assert.Equal(t, completed, data["status"])
	assert.NotEmpty(t, data["ended"])
	assert.Equal(t, bson.M{"bar": "baz"}, data["result"])
}

func TestCollectionFail(t *testing.T) {
	dbc := db.C("test-coll-fail")
	jqc := Wrap(dbc)

	err := jqc.Enqueue("foo", bson.M{"bar": "baz"})
	assert.NoError(t, err)

	job, err := jqc.Dequeue("foo")
	assert.NoError(t, err)
	assert.NotNil(t, job)

	err = jqc.Fail(job.ID, "some error")
	assert.NoError(t, err)

	var data bson.M
	err = dbc.FindId(job.ID).Select(bson.M{"_id": 0}).One(&data)
	assert.NoError(t, err)
	assert.Equal(t, failed, data["status"])
	assert.NotEmpty(t, data["ended"])
	assert.Equal(t, "some error", data["error"])
}

func TestCollectionCancel(t *testing.T) {
	dbc := db.C("test-coll-cancel")
	jqc := Wrap(dbc)

	err := jqc.Enqueue("foo", bson.M{"bar": "baz"})
	assert.NoError(t, err)

	job, err := jqc.Dequeue("foo")
	assert.NoError(t, err)
	assert.NotNil(t, job)

	err = jqc.Cancel(job.ID, "some reason")
	assert.NoError(t, err)

	var data bson.M
	err = dbc.FindId(job.ID).Select(bson.M{"_id": 0}).One(&data)
	assert.NoError(t, err)
	assert.Equal(t, cancelled, data["status"])
	assert.NotEmpty(t, data["ended"])
	assert.Equal(t, "some reason", data["reason"])
}

func TestCollectionEnsureIndexes(t *testing.T) {
	dbc := db.C("test-coll-ensure-indexes")
	jqc := Wrap(dbc)

	assert.NoError(t, jqc.EnsureIndexes())
	assert.NoError(t, jqc.EnsureIndexes())
}
