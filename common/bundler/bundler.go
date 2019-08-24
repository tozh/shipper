package bundler

import (
	"context"
	"errors"
	"golang.org/x/sync/semaphore"
	"sync"
	"time"
)

const (
	DefaultTimeThreshold   = time.Second
	DefaultCountTreshold   = 10
	DefaultSizeThreshold   = 1e6 // 1M
	DefaultBundleSizeLimit = 1e7 // 10M
	DefaultBufferSizeLimit = 1e9 // 1G
)

var (
	ErrOversize = errors.New("item size exceeds item size limit")
	ErrOverflow = errors.New("bundler exceeds buffer size limit")
)

type Item interface {
	Size() int
}

type Bundler struct {
	TimeThreshold  time.Duration
	CountThreshold int
	SizeThreshold  int

	BundleSizeLimit int
	BufferSizeLimit int
	HandlerLimit    int

	ticket    *Ticket
	handle    func([]Item)
	items     []Item
	itemsZero []Item
	size      int

	timer         *time.Timer
	timerInitOnce *sync.Once
	timerRunning  bool
	timerCancel   chan struct{}

	mutex       *sync.Mutex
	sem         *semaphore.Weighted
	semInitOnce *sync.Once
}

func (b *Bundler) initSemaphore() {
	b.semInitOnce.Do(func() {
		b.sem = semaphore.NewWeighted(int64(b.BufferSizeLimit))
	})
}

func (b *Bundler) initTimer() {
	b.timerInitOnce.Do(func() {
		b.timer = time.NewTimer(b.TimeThreshold)
	})
}

// flushLocked should call with b.mutex locked
func (b *Bundler) flushLocked() {
	// cancel the timer function -- Flush()
	if b.timerRunning {
		// make sure b.timer has been initialized.
		// it can only be initialed once.
		b.initTimer()
		b.timer.Stop()
		// if b.timerRunning, means there is a gorountine is waiting for the timerCancel.
		b.timerCancel <- struct{}{}
		b.timerRunning = false
	}
	if len(b.items) == 0 {
		return
	}
	// clear items
	items := b.items
	b.items = b.itemsZero
	size := b.size
	b.size = 0
	ti := b.ticket.Next()
	go func() {
		defer func() {
			b.sem.Release(int64(size))
			b.ticket.Release(ti)
		}()
		b.ticket.Acquire(ti)
		b.handle(items)
	}()
}

func (b *Bundler) Flush() {
	b.mutex.Lock()
	b.flushLocked()
	b.mutex.Unlock()

	// all tickets < ti are either finished or active.
	ti := b.ticket.WhatIsNext()
	b.initSemaphore()
	// wait all those tickets < ti.
	b.ticket.WaitAll(ti)
}

func (b *Bundler) add(item Item) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// if adding the item exceed the bundle size limit, need flushLocked the current bundle.
	if b.BundleSizeLimit > 0 && b.size+item.Size() > b.BundleSizeLimit {
		b.flushLocked()
	}

	// add item
	b.items = append(b.items, item)
	b.size += item.Size()

	// set timer to call Flush
	if !b.timerRunning {
		// make sure b.timer has been initialized.
		// it can only be initialed once.
		b.initTimer()
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

	if len(b.items) >= b.CountThreshold {
		b.flushLocked()
	}

	if b.size >= b.SizeThreshold {
		b.flushLocked()
	}
}

// if buffer is full, TryAdd() will return ErrOverflow error
func (b *Bundler) TryAdd(item Item) error {
	if b.BundleSizeLimit > 0 && item.Size() > b.BundleSizeLimit {
		return ErrOversize
	}
	b.initSemaphore()
	if !b.sem.TryAcquire(int64(item.Size())) {
		return ErrOverflow
	}
	b.add(item)
	return nil
}

// if buffer is full, Add() will wait semaphore to release
func (b *Bundler) Add(ctx context.Context, item Item) error {
	if b.BundleSizeLimit > 0 && item.Size() > b.BundleSizeLimit {
		return ErrOversize
	}
	b.initSemaphore()

	// wait until got semaphore.
	// semaphore Acquire() is FIFO. there is no starvation in race condition.
	err := b.sem.Acquire(ctx, int64(item.Size()))
	if err != nil {
		return err
	}
	b.add(item)
	return nil
}

func NewBundler(handle func([]Item)) *Bundler {
	b := &Bundler{
		TimeThreshold:   DefaultTimeThreshold,
		CountThreshold:  DefaultCountTreshold,
		SizeThreshold:   DefaultSizeThreshold,
		BundleSizeLimit: DefaultBundleSizeLimit,
		BufferSizeLimit: DefaultBufferSizeLimit,
		HandlerLimit:    1,
		handle:          handle,
		itemsZero:       make([]Item, 0, (DefaultCountTreshold+1)>>1),
		timerInitOnce:   &sync.Once{},
		timerCancel:     make(chan struct{}),
		mutex:           &sync.Mutex{},
		sem:             nil,
		semInitOnce:     &sync.Once{},
	}
	b.items = b.itemsZero
	return b
}
