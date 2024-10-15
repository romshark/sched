package sched_test

//go:generate mockgen -source ./sched.go -destination ./internal/mock/mock_gen.go -package mock TimeProvider,Timer

import (
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/romshark/sched/v2"
	"github.com/romshark/sched/v2/internal/mock"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSchedule(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWith(0, tm, nil)

	start := time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC)

	// First job
	tm.EXPECT().
		Now().
		MaxTimes(2).
		Return(start)

	timer := mock.NewMockTimer(mc)
	tm.EXPECT().
		AfterFunc(sched.Hour, gomock.Any()).
		MaxTimes(1).
		Return(timer)

	ExpectJobs(t)

	callback := func() {
		// This function would normally be executed by the timer,
		// however, we only need to check whether the timer was created.
		panic("this should not be invoked")
	}

	j, err := sched.Schedule(sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j)

	ExpectJobs(t, Job{j, callback})

	// Second job
	timer.EXPECT().
		Stop().
		MaxTimes(1).
		Return(true)

	dur2 := 30 * sched.Minute

	timer2 := mock.NewMockTimer(mc)
	tm.EXPECT().
		Now().
		MaxTimes(2).
		Return(start)
	tm.EXPECT().
		AfterFunc(dur2, gomock.Any()).
		MaxTimes(1).
		Return(timer2)

	j2, err := sched.Schedule(dur2, callback)
	require.NoError(t, err)
	require.NotZero(t, j2)

	ExpectJobs(t, Job{j2, callback}, Job{j, callback})

	// Third job
	tm.EXPECT().
		Now().
		MaxTimes(2).
		Return(start)

	j3, err := sched.Schedule(2*sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j)

	ExpectJobs(t, Job{j2, callback}, Job{j, callback}, Job{j3, callback})
}

func TestScheduleImmediately(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWith(0, tm, nil)

	start := time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC)
	tm.EXPECT().
		Now().
		MaxTimes(2).
		Return(start)

	timer := mock.NewMockTimer(mc)
	tm.EXPECT().
		AfterFunc(sched.Hour, gomock.Any()).
		MaxTimes(1).
		Return(timer)

	ExpectJobs(t)

	var counter int64
	var wg sync.WaitGroup

	wg.Add(1)
	callback := func() {
		defer wg.Done()
		atomic.AddInt64(&counter, 1)
	}

	j, err := sched.Schedule(0, callback)
	require.NoError(t, err)
	require.Zero(t, j)

	ExpectJobs(t)

	wg.Wait()
	require.Equal(t, int64(1), atomic.LoadInt64(&counter))
}

func TestCancel(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWith(0, tm, nil)

	tm.EXPECT().
		Now().
		MaxTimes(2).
		Return(time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC))

	timer := mock.NewMockTimer(mc)
	timer.EXPECT().
		Stop().
		MaxTimes(1).
		Return(true)

	tm.EXPECT().
		AfterFunc(sched.Hour, gomock.Any()).
		MaxTimes(1).
		Return(timer)

	ExpectJobs(t)

	callback := func() {
		panic("this should not be invoked")
	}

	require.Equal(t, 0, sched.Len())
	j, err := sched.Schedule(sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j)

	ExpectJobs(t, Job{j, callback})

	require.True(t, sched.Cancel(j))

	ExpectJobs(t)
}

func TestCancelNoop(t *testing.T) {
	ExpectJobs(t)
	require.False(t, sched.Cancel(sched.Job(ksuid.New())))
	ExpectJobs(t)
}

func TestAdvanceTime(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWith(0, tm, nil)

	start := time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC)

	tm.EXPECT().
		Now().
		MaxTimes(1).
		Return(start)

	require.Equal(t, start, sched.Now())

	tm.EXPECT().
		Now().
		MaxTimes(2).
		Return(start)

	timer := mock.NewMockTimer(mc)
	timer.EXPECT().
		Stop().
		MaxTimes(1).
		Return(true)

	dueDur := sched.Hour
	originalDue := start.Add(dueDur)
	tm.EXPECT().
		AfterFunc(dueDur, gomock.Any()).
		MaxTimes(1).
		Return(timer)

	ExpectJobs(t)

	callback := func() {
		panic("this should not be invoked")
	}

	require.Equal(t, 0, sched.Len())
	j, err := sched.Schedule(sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j)

	ExpectJobs(t, Job{j, callback})

	advanceBy := 25 * time.Minute
	timer.EXPECT().
		Reset(originalDue.Sub(start.Add(advanceBy))).
		MaxTimes(1).
		Return(true)
	tm.EXPECT().
		Now().
		MaxTimes(1).
		Return(start)

	require.Equal(t, advanceBy, sched.AdvanceTime(advanceBy))
	require.Equal(t, advanceBy, sched.Offset())

	tm.EXPECT().
		Now().
		MaxTimes(1).
		Return(start)

	require.Equal(t, start.Add(advanceBy), sched.Now())

	elapsed := 5 * time.Minute
	advanceBy2 := 30 * time.Minute
	timer.EXPECT().
		Reset(originalDue.Sub(start.Add(elapsed + advanceBy + advanceBy2))).
		MaxTimes(1).
		Return(true)
	tm.EXPECT().
		Now().
		MaxTimes(1).
		Return(start.Add(elapsed))

	require.Equal(t, advanceBy+advanceBy2, sched.AdvanceTime(advanceBy2))
	require.Equal(t, advanceBy+advanceBy2, sched.Offset())

	tm.EXPECT().
		Now().
		MaxTimes(1).
		Return(start)

	require.Equal(t, start.Add(advanceBy+advanceBy2), sched.Now())
}

func TestAdvanceToNext(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWith(0, tm, nil)

	// ExpectJobs(t)

	start := time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC)

	var wg sync.WaitGroup

	// Add first job
	tm.EXPECT().
		Now().
		MaxTimes(4).
		Return(start)

	timer := mock.NewMockTimer(mc)
	timer.EXPECT().
		Stop().
		MaxTimes(1).
		Return(true)

	dueDur := sched.Hour
	tm.EXPECT().
		AfterFunc(dueDur, gomock.Any()).
		MaxTimes(2).
		Return(timer)

	callback1 := func() { wg.Done() }

	nextAfter1 := sched.Hour
	j1, err := sched.Schedule(nextAfter1, callback1)
	require.NoError(t, err)
	require.NotZero(t, j1)

	// Add second job

	callback2 := func() { wg.Done() }
	nextAfter2 := 2 * sched.Hour
	j2, err := sched.Schedule(nextAfter2, callback2)
	require.NoError(t, err)
	require.NotZero(t, j2)

	ExpectJobs(t, Job{j1, callback1}, Job{j2, callback2})

	wg.Add(1)

	// Advance to first job
	timer.EXPECT().
		Stop().
		MaxTimes(1).
		Return(true)
	tm.EXPECT().
		Now().
		MaxTimes(2).
		Return(start)

	offset, advancedBy := sched.AdvanceToNext()
	require.Equal(t, nextAfter1, offset)
	require.Equal(t, nextAfter1, advancedBy)

	// Wait until the first job has been executed
	wg.Wait()
	require.Equal(t, nextAfter1, sched.Offset())
	wg.Add(1)

	// Advance to second job
	timer.EXPECT().
		Stop().
		MaxTimes(1).
		Return(true)
	tm.EXPECT().
		Now().
		MaxTimes(2).
		Return(start)

	offset2, advancedBy2 := sched.AdvanceToNext()
	require.Equal(t, nextAfter2, offset2)
	require.Equal(t, nextAfter2-nextAfter1, advancedBy2)

	// Wait until the second job has been executed
	wg.Wait()
	require.Equal(t, nextAfter2, sched.Offset())
}

func TestAdvanceToNextNoop(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWith(0, tm, nil)

	ExpectJobs(t)

	offset, advancedBy := sched.AdvanceToNext()
	require.Equal(t, time.Duration(0), offset)
	require.Equal(t, time.Duration(0), advancedBy)
	require.Equal(t, time.Duration(0), sched.Offset())
}

func TestScanAfter(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWith(0, tm, nil)

	start := time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC)

	tm.EXPECT().Now().AnyTimes().Return(start)
	timer := mock.NewMockTimer(mc)
	tm.EXPECT().AfterFunc(gomock.Any(), gomock.Any()).AnyTimes().Return(timer)

	callback := func() { panic("this should not be invoked") }

	j1, err := sched.Schedule(sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j1)

	j2, err := sched.Schedule(2*sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j2)

	j3, err := sched.Schedule(3*sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j3)

	ExpectJobsAfter(t, sched.Job{},
		Job{j1, callback},
		Job{j2, callback},
		Job{j3, callback},
	)
	ExpectJobsAfter(t, j1,
		Job{j2, callback},
		Job{j3, callback},
	)
	ExpectJobsAfter(t, j2,
		Job{j3, callback},
	)
	ExpectJobsAfter(t, j3)
}

func TestScanInterrupt(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWith(0, tm, nil)

	start := time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC)

	tm.EXPECT().Now().AnyTimes().Return(start)
	timer := mock.NewMockTimer(mc)
	tm.EXPECT().AfterFunc(gomock.Any(), gomock.Any()).AnyTimes().Return(timer)

	callback := func() { panic("this should not be invoked") }

	j1, err := sched.Schedule(sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j1)

	j2, err := sched.Schedule(2*sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j2)

	j3, err := sched.Schedule(3*sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j3)

	count := 0
	ok := sched.Scan(sched.Job{}, func(job sched.Job, jobFn func()) bool {
		count++
		return false
	})
	require.True(t, ok)
	require.Equal(t, 1, count)
}

func TestScanAfterNotFound(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWith(0, tm, nil)

	start := time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC)

	tm.EXPECT().Now().AnyTimes().Return(start)
	timer := mock.NewMockTimer(mc)
	tm.EXPECT().AfterFunc(gomock.Any(), gomock.Any()).AnyTimes().Return(timer)

	callback := func() { panic("this should not be invoked") }

	j1, err := sched.Schedule(sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j1)

	j2, err := sched.Schedule(2*sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j2)

	j3, err := sched.Schedule(3*sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j3)

	count := 0
	ok := sched.Scan(
		sched.Job(ksuid.New()),
		func(job sched.Job, jobFn func()) bool {
			count++
			return true
		},
	)
	require.False(t, ok)
	require.Zero(t, count)
}

func TestScheduleAutoExec(t *testing.T) {
	s := sched.New(0)
	var wg sync.WaitGroup
	wg.Add(3)
	var executionOrder []int
	_, err := s.Schedule(time.Minute, func() {
		defer wg.Done()
		executionOrder = append(executionOrder, 2)
	})
	require.NoError(t, err)
	_, err = s.Schedule(2*time.Minute, func() {
		defer wg.Done()
		executionOrder = append(executionOrder, 3)
	})
	require.NoError(t, err)
	_, err = s.Schedule(1, func() {
		defer wg.Done()
		executionOrder = append(executionOrder, 1)
		require.Equal(t, 2*time.Minute, s.AdvanceTime(2*time.Minute))
	})
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, []int{1, 2, 3}, executionOrder)
}

type Job struct {
	sched.Job
	JobFn func()
}

func ExpectJobs(t *testing.T, expected ...Job) {
	require.Equal(t, len(expected), sched.Len())
	ExpectJobsAfter(t, sched.Job{}, expected...)
}

func ExpectJobsAfter(t *testing.T, after sched.Job, expected ...Job) {
	var actual []Job
	ok := sched.Scan(after, func(j sched.Job, jobFn func()) bool {
		actual = append(actual, Job{j, jobFn})
		return true
	})
	require.True(t, ok)

	require.Len(t, actual, len(expected))
	for i, x := range expected {
		actual := actual[i]
		require.Equal(
			t, x.Job.String(), actual.Job.String(),
			"unexpected job id at index %d", i,
		)
		EqualFn(
			t, x.JobFn, actual.JobFn,
			"unexpected job fn at index %d", i,
		)
	}
}

func EqualFn(t *testing.T, e, a func(), msgAndArgs ...any) {
	fe := runtime.FuncForPC(reflect.ValueOf(e).Pointer()).Name()
	fa := runtime.FuncForPC(reflect.ValueOf(a).Pointer()).Name()
	require.Equal(t, fe, fa, msgAndArgs...)
}
