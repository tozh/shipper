package bundler

import (
	"context"
	"errors"
	"golang.org/x/sync/semaphore"
	"math"
	"sync"
	"time"
)

const (
	DefaultTimeThreshold   = time.Second
	DefaultCountThreshold  = 10
	DefaultSizeThreshold   = 1e6 // 1M
	DefaultBundleSizeLimit = 1e7 // 10M
	DefaultBufferSizeLimit = 1e9 // 1G
)

var (
	ErrOversize = errors.New("item size exceeds item size limit")
	ErrOverflow = errors.New("bundler exceeds buffer size limit")
)

type Bundler struct {
	TimeThreshold  time.Duration
	CountThreshold int
	SizeThreshold  int

	BundleSizeLimit int
	BufferSizeLimit int
	HandlerLimit    int

	handler func([]interface{})

	bundle     []interface{}
	size       int
	bundleZero []interface{}

	// for timer
	timerInitOnce sync.Once
	timer         *time.Timer
	timerRunning  bool
	timerCancel   chan struct{}

	mutex       sync.Mutex
	semInitOnce sync.Once
	sem         *semaphore.Weighted

	// for ticket
	mutex4t     sync.Mutex
	cond4t      *sync.Cond
	active      map[uint64]struct{}
	next        uint64
	nextHandled uint64
}

// only initial semaphore once.
func (b *Bundler) initSemOnce() {
	b.semInitOnce.Do(func() {
		b.sem = semaphore.NewWeighted(int64(b.BufferSizeLimit))
	})
}

// only initial timer once.
func (b *Bundler) initTimerOnce() {
	b.timerInitOnce.Do(func() {
		b.timer = time.NewTimer(b.TimeThreshold)
	})
}

// if buffer is full, Add() will return ErrOverflow error
func (b *Bundler) Add(item interface{}, size int) error {
	if b.BundleSizeLimit > 0 && size > b.BundleSizeLimit {
		return ErrOversize
	}
	b.initSemOnce()
	if !b.sem.TryAcquire(int64(size)) {
		return ErrOverflow
	}
	b.add(item, size)
	return nil
}

// if buffer is full, AddWait() will wait semaphore to release
// AddWait blocks until space is available or ctx is done.
// Calls to Add and AddWait should not be mixed on the same Bundler.
func (b *Bundler) AddWait(ctx context.Context, item interface{}, size int) error {
	if b.BundleSizeLimit > 0 && size > b.BundleSizeLimit {
		return ErrOversize
	}
	b.initSemOnce()

	// wait until got semaphore.
	// semaphore Acquire() is FIFO. there is no starvation in race condition.
	err := b.sem.Acquire(ctx, int64(size))
	if err != nil {
		return err
	}
	b.add(item, size)
	return nil
}

func (b *Bundler) Flush() {
	b.mutex.Lock()
	b.flushLocked()
	b.mutex.Unlock()

	// all tickets < ti are either finished or active.
	t := b.next
	b.initSemOnce()
	// wait all those tickets < t.
	b.waitAll(t)
}

func (b *Bundler) add(item interface{}, size int) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// if adding the item exceed the bundle size limit, need flushLocked the current bundle.
	if b.BundleSizeLimit > 0 && b.size+size > b.BundleSizeLimit {
		b.flushLocked()
	}

	// add item
	b.bundle = append(b.bundle, item)
	b.size += size

	// set timer to call Flush()
	if !b.timerRunning {
		// make sure b.timer has been initialized.
		// it can only be initialed once.
		b.initTimerOnce()
		b.timer.Reset(b.TimeThreshold)
		b.timerRunning = true
		go func() {
			select {
			case <-b.timer.C:
				// why Flush(), not flushLocked():
				// Flush() waits all active tickets to be finished.
				// timer triggered means bundler is idle.
				// waiting has no damage to throughput.
				b.Flush()
			case <-b.timerCancel:
			}
		}()
	}

	if len(b.bundle) == b.CountThreshold {
		b.flushLocked()
	}

	if b.size >= b.SizeThreshold {
		b.flushLocked()
	}
}

// flushLocked should call with b.mutex locked
func (b *Bundler) flushLocked() {
	// stop timer
	if b.timerRunning {
		// don't need to initialize timer. b.timerRunning makes sure timer has been initialized

		// if stopped by calling Stop()
		// it means the timer hasn't been triggered and a gorountine is waiting for timerCancel.
		if b.timer.Stop() {
			b.timerCancel <- struct{}{}
		}
		b.timerRunning = false
	}
	if len(b.bundle) == 0 {
		return
	}
	// copy and clear bundle
	bundle := b.bundle
	b.bundle = b.bundleZero
	size := b.size
	b.size = 0
	ticket := b.next
	b.next++
	go func() {
		defer func() {
			b.sem.Release(int64(size))
			b.release(ticket)
		}()
		b.acquire(ticket)
		b.handler(bundle)
	}()
}

func (b *Bundler) acquire(ticket uint64) {
	b.mutex4t.Lock()
	defer b.mutex4t.Unlock()

	if ticket < b.nextHandled {
		panic("ticket: acquire: ticket arg too small")
	}
	for !(ticket == b.nextHandled && len(b.active) < b.HandlerLimit) {
		b.cond4t.Wait()
	}
	b.active[ticket] = struct{}{}
	b.nextHandled++
	b.cond4t.Broadcast()
}

func (b *Bundler) release(ticket uint64) {
	b.mutex4t.Lock()
	defer b.mutex4t.Unlock()

	_, in := b.active[ticket]
	if !in {
		panic("ticket: release: not an active ticket")
	}
	delete(b.active, ticket)
	b.cond4t.Broadcast()
}

func (b *Bundler) waitAll(n uint64) {
	b.mutex4t.Lock()
	defer b.mutex4t.Unlock()

	for !(b.nextHandled >= n && n <= min(b.active)) {
		b.cond4t.Wait()
	}
}

// min returns the minimum value of the set s, or the largest uint64 if
// s is empty.
func min(s map[uint64]struct{}) uint64 {
	var m uint64 = math.MaxUint64
	for n := range s {
		if n < m {
			m = n
		}
	}
	return m
}

func NewBundler(handler func([]interface{})) *Bundler {
	b := &Bundler{
		TimeThreshold:   DefaultTimeThreshold,
		CountThreshold:  DefaultCountThreshold,
		SizeThreshold:   DefaultSizeThreshold,
		BundleSizeLimit: DefaultBundleSizeLimit,
		BufferSizeLimit: DefaultBufferSizeLimit,
		HandlerLimit:    1,
		handler:         handler,
		timerInitOnce:   sync.Once{},
		bundleZero:      make([]interface{}, 0),
		timerCancel:     make(chan struct{}),
		mutex:           sync.Mutex{},
		semInitOnce:     sync.Once{},
		mutex4t:         sync.Mutex{},
		active:          make(map[uint64]struct{}),
	}
	b.cond4t = sync.NewCond(&b.mutex4t)
	b.bundle = b.bundleZero
	return b
}
