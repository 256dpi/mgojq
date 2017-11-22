package mgojq

import (
	"time"

	"gopkg.in/tomb.v2"
)

// Worker is a function that processes a job. The function must complete, fail
// or cancel the job on its own. If the provided channel is closes the worker
// should immediately finish the job and cancel long running jobs.
type Worker func(c *Collection, j *Job, quit <-chan struct{}) error

// Pool manages multiple goroutines that dequeue jobs.
type Pool struct {
	Timeout time.Duration

	size    int
	workers map[string]Worker
	names   []string
	jobs    chan *Job

	started bool
	coll    *Collection

	tomb tomb.Tomb
}

// NewPool will create a new pool.
func NewPool(size int) *Pool {
	return &Pool{
		Timeout: 1 * time.Second,
		size:    size,
		jobs:    make(chan *Job),
	}
}

// Register will register the specified worker for the specified job name.
func (p *Pool) Register(name string, worker Worker) {
	// add name if missing
	if _, ok := p.workers[name]; !ok {
		p.names = append(p.names, name)
	}

	// set or update worker
	p.workers[name] = worker
}

// Start will start the worker pool.
func (p *Pool) Start(coll *Collection) {
	// check flag
	if p.started {
		panic("pool can only be started once")
	}

	// set flag
	p.started = true

	// set collection
	p.coll = coll

	// run dequeuer
	p.tomb.Go(p.dequeuer)
}

// Close will wait for current jobs to finish, close the pool and return eventual
// errors.
func (p *Pool) Close() error {
	// kill tomb
	p.tomb.Kill(nil)

	// wait for clean exit
	return p.tomb.Wait()
}

// TODO: How to catch errors?

func (p *Pool) dequeuer() error {
	// run workers
	for i := 0; i < p.size; i++ {
		p.tomb.Go(p.worker)
	}

	for {
	dequeue:
		// dequeue next job
		job, err := p.coll.Dequeue(p.names...)
		if err != nil {
			return err
		} else if job == nil {
			goto wait
		}

		// queue job for processing
		p.jobs <- job

		// get next job
		goto dequeue

	wait:
		// wait
		select {
		case <-p.tomb.Dying():
			return tomb.ErrDying
		case <-time.After(p.Timeout):
			goto dequeue
		}
	}
}

func (p *Pool) worker() error {
	for {
		// wait
		select {
		case <-p.tomb.Dying():
			return tomb.ErrDying
		case job := <-p.jobs:
			// get function
			fn := p.workers[job.Name]

			// call function
			err := fn(p.coll, job, p.tomb.Dying())
			if err != nil {
				return err
			}
		}
	}
}
