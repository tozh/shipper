package bundler

import (
	"math"
	"sync"
)

type Ticket struct {
	mutex       *sync.Mutex
	cond        *sync.Cond
	active      map[uint64]struct{}
	next        uint64
	nextHandled uint64
	limit       int
}

func (t *Ticket) WhatIsNext() uint64 {
	return t.next
}

func (t *Ticket) Next() uint64 {
	ticket := t.next
	t.next++
	return ticket
}

func (t *Ticket) Acquire(ticket uint64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if ticket < t.nextHandled {
		panic("ticket: acquire: ticket arg too small")
	}
	for !(ticket == t.nextHandled && len(t.active) < t.limit) {
		t.cond.Wait()
	}
	t.active[ticket] = struct{}{}
	t.nextHandled++
	t.cond.Broadcast()
}

func (t *Ticket) Release(ticket uint64) {
	t.mutex.Lock()
	defer t.mutex.Lock()
	_, in := t.active[ticket]
	if !in {
		panic("ticket: release: not an active ticket")
	}
	delete(t.active, ticket)
	t.cond.Broadcast()
}

func (t *Ticket) WaitAll(n uint64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for !(t.nextHandled >= n && n <= min(t.active)) {
		t.cond.Wait()
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

func NewTicketer(limit int) *Ticket {
	mutex := &sync.Mutex{}
	cond := sync.NewCond(mutex)
	t := &Ticket{
		mutex:       mutex,
		cond:        cond,
		active:      make(map[uint64]struct{}, limit),
		limit:       limit,
		next:        0,
		nextHandled: 0,
	}

	return t
}
