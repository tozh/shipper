package worker

type Job interface {
	Do()
}

type Worker interface {
	Start() // Start() has a for loop, if want to Start() as daemon, go Start()
	Assigned(job Job)
	DoneAction()
	Done()
}

func NewWorker(workshop chan Worker) Worker {
	w := worker{
		Workshop: workshop,
		WorkLine: make(chan Job),
		done:     make(chan struct{}),
	}
	return &w
}

type worker struct {
	Workshop chan Worker
	WorkLine chan Job
	done     chan struct{}
}

// Start() has a for loop, if want to Start() as daemon, go Start()
func (w *worker) Start() {
	for {
		// register the worker's work line as available
		w.Workshop <- w
		select {
		case job := <-w.WorkLine:
			job.Do()

		case <-w.done:
			w.DoneAction()
			return
		}
	}
}


func (w *worker) Assigned(job Job) {
	w.WorkLine <- job
}

func (w *worker) DoneAction() {
	return
}

func (w *worker) Done() {
	// TODO: use goroutine or not?
	go func() {
		close(w.done)
	}()
	//close(w.done)
}
