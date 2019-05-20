package tail

import (
	"github.com/fsnotify/fsnotify"
	"github.com/labstack/gommon/log"
)




type Watcher struct {
	watcher *fsnotify.Watcher
	files   map[string]*FileTracker
	done    chan struct{}
}

func (w *Watcher) Add(filename string) error {
	err := w.watcher.Add(filename)
	return err
}

func (w *Watcher) Run() {
	defer w.Done()
	go func() {
		for {
			select {
			case ev, ok := <-w.watcher.Events:
				if !ok {
					log.Info("goroutine returned 1")
					return
				}
				if isOp(ev, fsnotify.Create) {
					log.Info("Create", ev.Name)
				}
				if isOp(ev, fsnotify.Write) {
					log.Info("Write", ev.Name)
				}
				if isOp(ev, fsnotify.Remove) {
					log.Info("Remove", ev.Name)
				}
				if isOp(ev, fsnotify.Rename) {
					log.Info("Rename", ev.Name)
				}
			case err, ok := <-w.watcher.Errors:
				if !ok {
					log.Info("goroutine returned 2")
					return
				}
				log.Info(err)
			}
		}
	}()
	<-w.done
}

func (w *Watcher) Done() {
	w.done <- struct{}{}
	w.watcher.Close()
}

func isOp(ev fsnotify.Event, op fsnotify.Op) bool {
	return ev.Op&op == op
}

func NewWatcher() *Watcher {
	watch, err := fsnotify.NewWatcher()
	if err != nil {
		log.Panic(err)
	}
	w := &Watcher{
		watcher: watch,
		done:  make(chan struct{}),
	}
	return w
}
