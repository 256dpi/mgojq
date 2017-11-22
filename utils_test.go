package mgojq

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var db *mgo.Database

func init() {
	// create session
	sess, err := mgo.Dial("mongodb://localhost/test-mgojq")
	if err != nil {
		panic(err)
	}

	// save db reference
	db = sess.DB("")

	// drop database
	err = db.DropDatabase()
	if err != nil {
		panic(err)
	}

	// force recreation
	err = db.C("foo").Insert(bson.M{"foo": "bar"})
	if err != nil {
		panic(err)
	}
}

var setTime = time.Now()

func replaceTimeSlice(s []bson.M) []bson.M {
	for _, m := range s {
		replaceTimeMap(m)
	}

	return s
}

func replaceTimeMap(m bson.M) bson.M {
	for key, value := range m {
		if v, ok := value.(bson.M); ok {
			replaceTimeMap(v)
		} else if v, ok := value.([]bson.M); ok {
			replaceTimeSlice(v)
		} else if v, ok := value.(time.Time); ok && !v.IsZero() {
			m[key] = setTime
		}
	}

	return m
}

func replaceTimeJob(j *Job) *Job {
	if !j.Created.IsZero() {
		j.Created = setTime
	}

	return j
}
