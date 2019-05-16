package tail

import (
	"bufio"
	"github.com/fsnotify/fsnotify"
	"github.com/labstack/gommon/log"
	"github.com/zhaotong0312/shipper/common/message"
	"io"
	"os"
)

type FileTracker struct {
	filename string
	file     *os.File
	fileInfo os.FileInfo
	config   FileTrackerConfig
	watcher  *fsnotify.Watcher
	bus      chan message.Message
	notify   chan fsnotify.Event
	done     chan struct{}
}

// TODO when rotation happens, delay close file time
// TODO fileMustExist, pre-assign empty FileTracker or only when the file exists
type FileTrackerConfig struct {
	readFromStart bool
	//fileMustExist bool
	//delayCloseTime time.Time
}

// event filter
func (ft *FileTracker) poll() {
	for {
		select {
		case ev := <-ft.watcher.Events:
			log.Debug(ev.String())
			if isOp(ev, fsnotify.Write) {
				ft.notify <- ev
			} else if isOp(ev, fsnotify.Rename) || isOp(ev, fsnotify.Remove) {
				ft.close()
				return
			}
		case err := <-ft.watcher.Errors:
			log.Fatal(err)
		}
	}
}

func (ft *FileTracker) init() error {
	var err error
	ft.file, err = os.Open(ft.filename)
	if err != nil {
		return err
	}
	ft.fileInfo, err = os.Stat(ft.filename)
	if err != nil {
		return err
	}

	if !ft.config.readFromStart {
		n, err := ft.file.Seek(0, io.SeekEnd)
		log.Debugf("seek to the end, result: %d, error: %v\n", n, err)
	}

	ft.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	err = ft.watcher.Add(ft.filename)
	if err != nil {
		return err
	}
	return nil
}

// TODO: How to deal with truncate
func (ft *FileTracker) run() {
	reader := bufio.NewReader(ft.file)
	defer ft.file.Close()

	go ft.poll()

	for {
		lineBytes, err := reader.ReadBytes('\n')
		ft.bus <- NewLine(lineBytes, DefaultLevelForLine)
		if err != nil {
			if err == io.EOF {
				// TODO read outside notify or inside notify
				select {
				case ev := <-ft.notify:
					log.Debug(ev.String())
				case <-ft.done:
					log.Debug("Run() returned")
					return
				}
			} else {
				log.Fatal(err)
			}
		}
	}
}

func (ft *FileTracker) close() {
	close(ft.done)
}

func (ft *FileTracker) Run() {
	go ft.run()
}

func NewFileTracker(filename string, config FileTrackerConfig, bus chan message.Message) (*FileTracker, error) {
	ft := &FileTracker{
		filename: filename,
		file:     nil,
		fileInfo: nil,
		config:   config,
		watcher:  nil,
		bus:      bus,
		notify:   make(chan fsnotify.Event),
		done:     make(chan struct{}),
	}
	err := ft.init()
	if err != nil {
		return nil, err
	}
	return ft, nil
}
