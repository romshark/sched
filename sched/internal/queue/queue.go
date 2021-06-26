package queue

import (
	"github.com/huandu/skiplist"
	"github.com/segmentio/ksuid"
)

func New() Queue {
	return Queue{
		l: skiplist.New(
			skiplist.GreaterThanFunc(func(a, b interface{}) int {
				s1, s2 := a.(ksuid.KSUID).String(), b.(ksuid.KSUID).String()
				if s1 > s2 {
					return 1
				} else if s1 < s2 {
					return -1
				}
				return 0
			}),
		),
	}
}

type Queue struct {
	l *skiplist.SkipList
}

func (q Queue) Set(id ksuid.KSUID, fn func()) (setAtFront bool) {
	e := q.l.Set(id, job{ID: id, Fn: fn})
	return e.Prev() == nil
}

func (q Queue) Get(id ksuid.KSUID) func() {
	if e := q.l.Get(id); e != nil {
		return e.Value.(job).Fn
	}
	return nil
}

func (q Queue) Front() (ksuid.KSUID, func()) {
	if e := q.l.Front(); e != nil {
		v := e.Value.(job)
		return v.ID, v.Fn
	}
	return ksuid.KSUID{}, nil
}

func (q Queue) Remove(id ksuid.KSUID) (removed bool) {
	e := q.l.Remove(id)
	return e != nil
}

func (q Queue) Len() int {
	return q.l.Len()
}

func (q Queue) Scan(
	after ksuid.KSUID,
	fn func(ksuid.KSUID, func()) bool,
) (afterFound bool) {
	var start *skiplist.Element
	var zero ksuid.KSUID
	if after != zero {
		if start = q.l.Get(after); start == nil {
			return false
		}
		start = start.Next()
	} else {
		start = q.l.Front()
	}

	for e := start; e != nil; e = e.Next() {
		j := e.Value.(job)
		if !fn(j.ID, j.Fn) {
			return true
		}
	}
	return true
}

// job is a job descriptor.
type job struct {
	ID ksuid.KSUID
	Fn func()
}
