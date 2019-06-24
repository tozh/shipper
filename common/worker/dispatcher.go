package worker

func NewDispatcher(jobQueue chan Job, maxWorkers int) *Dispatcher {
	dispatcher := Dispatcher{
		JobQueue:   jobQueue,
		MaxWorkers: maxWorkers,
		Workers:    make([]Worker, maxWorkers),
		Workshop:   make(chan Worker, maxWorkers),
	}

	for i := 0; i < dispatcher.MaxWorkers; i++ {
		worker := NewWorker(dispatcher.Workshop)
		dispatcher.Workers[i] = worker
	}
	return &dispatcher
}

type Dispatcher struct {
	JobQueue   chan Job
	MaxWorkers int
	Workers    []Worker
	Workshop   chan Worker
	done       chan struct{}
}

// Start() has a for loop, if want to Start() as daemon, go Start()
func (d *Dispatcher) Start() {
	// start workers
	for _, worker := range d.Workers {
		go worker.Start()
	}
	d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.JobQueue:
			// a job request has been received

			// TODO: use goroutine or not?
			go func(job Job) {
				// get an available worker .
				worker := <-d.Workshop
				// worker is assigned the job
				worker.Assigned(job)
			}(job)
		case <-d.done:
			d.DoneAction()
			return
		}
	}
}

func (d *Dispatcher) Assigned(job Job) {
	d.JobQueue <- job
}

func (d *Dispatcher) DoneAction() {
	for _, worker := range d.Workers {
		worker.Done()
	}
}

func (d *Dispatcher) Done() {
	// TODO: use goroutine or not?
	go func() {
		close(d.done)
	}()
	//close(d.done)
}
