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
			"status": "enqueued",
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
			"status": "enqueued",
		},
		{
			"name": "foo",
			"params": bson.M{
				"bar": 1,
			},
			"status": "enqueued",
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
	assert.Equal(t, dequeued, job.Status)

	job, err = jqc.Dequeue("foo")
	assert.NoError(t, err)
	assert.Nil(t, job)
}

func TestCollectionDequeuePanic(t *testing.T) {
	dbc := db.C("test-coll-dequeue")
	jqc := Wrap(dbc)

	assert.Panics(t, func() {
		jqc.Dequeue()
	})
}

func TestCollectionEnsureIndexes(t *testing.T) {
	dbc := db.C("test-coll-ensure-indexes")
	jqc := Wrap(dbc)

	assert.NoError(t, jqc.EnsureIndexes())
	assert.NoError(t, jqc.EnsureIndexes())
}