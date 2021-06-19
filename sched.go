package sched

import (
	"fmt"
	"sync"
	"time"

	"github.com/huandu/skiplist"
	"github.com/segmentio/ksuid"
)

// DefaultScheduler is the default Scheduler
// used by Schedule, Cancel, Now, AdvanceTime, Len and Scan.
var DefaultScheduler = New(0)

// Now returns the current time of the scheduler considering the offset.
func Now() time.Time {
	return DefaultScheduler.Now()
}

// Schedule schedules fn for execution at the scheduler's time.
// fn will be executed in its own goroutine.
// if n < 1 then fn will be executed immediately and
// the returned JobID will be zero.
func Schedule(in time.Duration, fn func()) (Job, error) {
	return DefaultScheduler.Schedule(in, fn)
}

// Cancel canceles a pending job and returns true.
// Returns false if no job was canceled.
func Cancel(id Job) bool {
	return DefaultScheduler.Cancel(id)
}

// AdvanceTime advances the current time by the given duration.
func AdvanceTime(by time.Duration) (newOffset time.Duration) {
	return DefaultScheduler.AdvanceTime(by)
}

// Len returns the lenfth of the queue (number of pending jobs).
func Len() int {
	return DefaultScheduler.Len()
}

// Scan scans all jobs after the given job executing fn for each
// until either the end of the queue is reached or fn returns false.
// Starts from the front of the queue if after is zero.
// Returns false if after doesn't exist, otherwise returns true.
func Scan(after Job, fn func(job Job, jobFn func()) bool) (ok bool) {
	return DefaultScheduler.Scan(after, fn)
}

// New creates a new scheduler with the given time offset.
func New(timeOffset time.Duration) *Scheduler {
	t := time.NewTimer(time.Hour)
	t.Stop()
	return &Scheduler{
		timeOffset: timeOffset,
		queue: skiplist.New(
			skiplist.GreaterThanFunc(func(a, b interface{}) int {
				s1, s2 := a.(Job).String(), b.(Job).String()
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

// Scheduler is a job scheduler.
type Scheduler struct {
	lock       sync.RWMutex
	timeOffset time.Duration
	queue      *skiplist.SkipList
	scheduled  struct {
		job
		*time.Timer
	}
}

// Now returns the current time of the scheduler considering the offset.
func (s *Scheduler) Now() time.Time {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.now()
}

// Schedule schedules fn for execution at the scheduler's time.
// fn will be executed in its own goroutine.
// if n < 1 then fn will be executed immediately and
// the returned JobID will be zero.
func (s *Scheduler) Schedule(in time.Duration, fn func()) (Job, error) {
	if in < 1 {
		// Execute immediately
		go fn()
		return Job{}, nil
	}

	id, err := newJobID(s.now().Add(in))
	if err != nil {
		return Job{}, fmt.Errorf("generating unique KSUID: %w", err)
	}
	j := job{ID: id, Fn: fn}

	s.lock.Lock()
	defer s.lock.Unlock()

	if e := s.queue.Get(id); e != nil {
		return Job{}, fmt.Errorf("identifier collision: %s", id.String())
	}

	e := s.queue.Set(id, j)
	if e.Prev() != nil {
		return id, nil
	}

	s.execute(j)

	return id, nil
}

// Cancel canceles a pending job and returns true.
// Returns false if no job was canceled.
func (s *Scheduler) Cancel(id Job) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	e := s.queue.Remove(id)
	if e == nil {
		return false
	}

	if e.Value.(job).ID == s.scheduled.ID {
		// Canceled currently scheduled job
		s.scheduleFirstFromQueue()
	}
	return true
}

// AdvanceTime advances the current time by the given duration.
func (s *Scheduler) AdvanceTime(by time.Duration) (newOffset time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.timeOffset += by

	if s.scheduled.Timer != nil {
		s.scheduled.Timer.Reset(s.scheduled.ID.Due().Sub(s.now()))
	}
	return s.timeOffset
}

// Len returns the lenfth of the queue (number of pending jobs).
func (s *Scheduler) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.queue.Len()
}

// Scan scans all jobs after the given job executing fn for each
// until either the end of the queue is reached or fn returns false.
// Starts from the front of the queue if after is zero.
// Returns false if after doesn't exist, otherwise returns true.
func (s *Scheduler) Scan(
	after Job,
	fn func(job Job, jobFn func()) bool,
) (ok bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var start *skiplist.Element
	var zero Job
	if after != zero {
		if start = s.queue.Get(after); start == nil {
			return false
		}
	} else {
		start = s.queue.Front()
	}

	for e := start; e != nil; e = e.Next() {
		j := e.Value.(job)
		if !fn(j.ID, j.Fn) {
			return true
		}
	}
	return true
}

// execute either executes the job immediately
// or schedules the job for deferred execution.
func (s *Scheduler) execute(j job) {
	e := func() {
		j.Fn()

		s.lock.Lock()
		defer s.lock.Unlock()

		s.queue.Remove(j.ID)

		// Schedule next if any
		s.scheduleFirstFromQueue()
	}

	if s.scheduled.Timer != nil {
		s.scheduled.Timer.Stop()
	}
	s.scheduled.job = j
	d := j.ID.Due().Sub(s.now())
	if d < 1 {
		// Execute immediately
		s.scheduled.Timer = nil
		go e()
	} else {
		// Schedule for deferred execution
		s.scheduled.Timer = time.AfterFunc(d, e)
	}
}

// scheduleFirstFromQueue takes the first job from the queue
// and schedules it for execution.
func (s *Scheduler) scheduleFirstFromQueue() {
	if n := s.queue.Front(); n != nil {
		s.execute(n.Value.(job))
	}
}

// now returns the current time considering the offset.
func (s *Scheduler) now() time.Time {
	return time.Now().Add(s.timeOffset)
}

// newJobID generates a new unique identifier.
func newJobID(tm time.Time) (Job, error) {
	k, err := ksuid.NewRandomWithTime(tm)
	if err != nil {
		return Job{}, err
	}
	return Job(k), nil
}

// job is a job descriptor.
type job struct {
	ID Job
	Fn func()
}

// Job is a unique job identifier.
type Job ksuid.KSUID

// String returns the stringified identifier.
func (id Job) String() string {
	return ksuid.KSUID(id).String()
}

// Due returns the scheduled due time of the job.
func (id Job) Due() time.Time {
	return ksuid.KSUID(id).Time()
}
