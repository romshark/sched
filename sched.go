package sched

import (
	"fmt"
	"sync"
	"time"

	"github.com/romshark/sched/v2/internal/queue"

	"github.com/segmentio/ksuid"
)

type (
	Time     = time.Time
	Duration = time.Duration
)

const (
	Nanosecond  = time.Nanosecond
	Microsecond = time.Microsecond
	Millisecond = time.Millisecond
	Second      = time.Second
	Minute      = time.Minute
	Hour        = time.Hour
)

type Timer interface {
	Stop() bool
	Reset(Duration) bool
}

type TimeProvider interface {
	Now() Time
	AfterFunc(Duration, func()) Timer
}

type QueueReader interface {
	Has(ksuid.KSUID) bool
	Len() int
	Scan(
		after ksuid.KSUID,
		fn func(ksuid.KSUID, func()) bool,
	) (afterFound bool)
}

type QueueWriter interface {
	Set(ksuid.KSUID, func()) (setAtFront bool)
	Front() (ksuid.KSUID, func())
	Remove(ksuid.KSUID) (ok bool)
}

type QueueReadWriter interface {
	QueueReader
	QueueWriter
}

// DefaultScheduler is the default Scheduler
// used by Schedule, Cancel, Now, AdvanceTime,
// AdvanceToNext, Len, Offset and Scan.
var DefaultScheduler = New(0)

// Now returns the current time of the scheduler considering the offset.
func Now() Time {
	return DefaultScheduler.Now()
}

// Schedule schedules fn for execution at the scheduler's time.
// fn will be executed in its own goroutine.
// if in < 1 then fn will be executed immediately and
// the returned JobID will be zero.
func Schedule(in Duration, fn func()) (Job, error) {
	return DefaultScheduler.Schedule(in, fn)
}

// Cancel cancels a pending job and returns true.
// Returns false if no job was canceled.
func Cancel(id Job) bool {
	return DefaultScheduler.Cancel(id)
}

// AdvanceTime advances the current time by the given duration.
func AdvanceTime(by Duration) (newOffset Duration) {
	return DefaultScheduler.AdvanceTime(by)
}

// AdvanceToNext advances the current time to the next job
// executing it immediately. Does nothing if no jobs are pending.
func AdvanceToNext() (newOffset, advancedBy Duration) {
	return DefaultScheduler.AdvanceToNext()
}

// Len returns the length of the queue (number of pending jobs).
func Len() int {
	return DefaultScheduler.Len()
}

// Offset returns the scheduler's time offset.
func Offset() time.Duration {
	return DefaultScheduler.Offset()
}

// Scan scans all jobs after the given job executing fn for each
// until either the end of the queue is reached or fn returns false.
// Starts from the front of the queue if after is zero.
// Returns false if after doesn't exist, otherwise returns true.
func Scan(after Job, fn func(job Job, jobFn func()) bool) (ok bool) {
	return DefaultScheduler.Scan(after, fn)
}

// New creates a new scheduler with the given time offset.
func New(timeOffset Duration) *Scheduler {
	return NewWith(timeOffset, nil, nil)
}

// NewWith is similar to New but replaces the default time provider
// and queue implementation.
// If t == nil then standard time package is used by default.
// If q == nil then sched/internal/queue.Queue is used by default.
func NewWith(
	timeOffset Duration,
	t TimeProvider,
	q QueueReadWriter,
) *Scheduler {
	if t == nil {
		t = timeProvider{}
	}
	if q == nil {
		q = queue.New()
	}
	return &Scheduler{
		provider:   t,
		queue:      q,
		timeOffset: timeOffset,
	}
}

// Scheduler is a job scheduler.
type Scheduler struct {
	provider   TimeProvider
	lock       sync.RWMutex
	timeOffset Duration
	queue      QueueReadWriter
	scheduled  struct {
		ID Job
		Fn func()
		Timer
	}
}

// Now returns the current time of the scheduler considering the offset.
func (s *Scheduler) Now() Time {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.now()
}

// Schedule schedules fn for execution at the scheduler's time.
// fn will be executed in its own goroutine.
// if in < 1 then fn will be executed immediately and
// the returned JobID will be zero.
func (s *Scheduler) Schedule(in Duration, fn func()) (Job, error) {
	if in < 1 {
		// Execute immediately
		go fn()
		return Job{}, nil
	}

	id, err := newJobID(s.now().Add(in))
	if err != nil {
		return Job{}, fmt.Errorf("generating unique KSUID: %w", err)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if s.queue.Has(ksuid.KSUID(id)) {
		return Job{}, fmt.Errorf("identifier collision: %s", id.String())
	}

	if !s.queue.Set(ksuid.KSUID(id), fn) {
		return id, nil
	}

	s.execute(id, fn)

	return id, nil
}

// Cancel cancels a pending job and returns true.
// Returns false if no job was canceled.
func (s *Scheduler) Cancel(id Job) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.queue.Remove(ksuid.KSUID(id)) {
		return false
	}

	if id == s.scheduled.ID {
		// Canceled currently scheduled job
		s.scheduleFirstFromQueue()
	}
	return true
}

// AdvanceTime advances the current time by the given duration.
func (s *Scheduler) AdvanceTime(by Duration) (newOffset Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.timeOffset += by

	if s.scheduled.Timer != nil {
		s.scheduled.Timer.Reset(s.scheduled.ID.Due().Sub(s.now()))
	}
	return s.timeOffset
}

// AdvanceToNext advances the current time to the next job
// executing it immediately. Does nothing if no jobs are pending.
func (s *Scheduler) AdvanceToNext() (newOffset, advancedBy Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.scheduled.Timer == nil {
		return s.timeOffset, 0
	}

	by := s.scheduled.ID.Due().Sub(s.now())
	s.timeOffset += by

	e := s.makeExecutable(s.scheduled.ID, s.scheduled.Fn)

	if s.scheduled.Timer != nil {
		s.scheduled.Timer.Stop()
		s.scheduled.Timer = nil
	}

	// Execute immediately
	go e()

	return s.timeOffset, by
}

// Offset returns the scheduler's time offset.
func (s *Scheduler) Offset() time.Duration {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.timeOffset
}

// Len returns the length of the queue (number of pending jobs).
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

	return s.queue.Scan(
		ksuid.KSUID(after),
		func(id ksuid.KSUID, job func()) bool {
			return fn(Job(id), job)
		},
	)
}

func (s *Scheduler) makeExecutable(id Job, fn func()) func() {
	return func() {
		fn()

		s.lock.Lock()
		defer s.lock.Unlock()

		s.queue.Remove(ksuid.KSUID(id))

		// Schedule next if any
		s.scheduleFirstFromQueue()
	}
}

// execute either executes the job immediately
// or schedules the job for deferred execution.
func (s *Scheduler) execute(id Job, fn func()) {
	e := s.makeExecutable(id, fn)

	if s.scheduled.Timer != nil {
		s.scheduled.Timer.Stop()
	}
	s.scheduled.ID, s.scheduled.Fn = id, fn
	d := id.Due().Sub(s.now())
	if d < 1 {
		// Execute immediately
		s.scheduled.Timer = nil
		go e()
	} else {
		// Schedule for deferred execution
		s.scheduled.Timer = s.provider.AfterFunc(d, e)
	}
}

// scheduleFirstFromQueue takes the first job from the queue
// and schedules it for execution.
func (s *Scheduler) scheduleFirstFromQueue() {
	id, fn := s.queue.Front()
	if fn != nil {
		s.execute(Job(id), fn)
	}
}

// now returns the current time considering the offset.
func (s *Scheduler) now() Time {
	return s.provider.Now().Add(s.timeOffset)
}

// newJobID generates a new unique identifier.
func newJobID(tm Time) (Job, error) {
	k, err := ksuid.NewRandomWithTime(tm)
	if err != nil {
		return Job{}, err
	}
	return Job(k), nil
}

// Job is a unique job identifier.
type Job ksuid.KSUID

// String returns the stringified identifier.
func (id Job) String() string {
	return ksuid.KSUID(id).String()
}

// Due returns the scheduled due time of the job.
func (id Job) Due() Time {
	return ksuid.KSUID(id).Time()
}
