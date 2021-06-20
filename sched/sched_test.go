package sched_test

//go:generate mockgen -package mock -destination ./mock/mock_gen.go . TimeProvider,Timer

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/romshark/sched/sched"
	"github.com/romshark/sched/sched/mock"
	"github.com/stretchr/testify/require"
)

func TestSchedule(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWithProvider(0, tm)

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

	callback := func() {
		// This function would normally be executed by the timer,
		// however, we only need to check whether the timer was created.
		panic("this should not be invoked")
	}

	j, err := sched.Schedule(sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j)

	ExpectJobs(t, Job{j, callback})

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
}

func TestCancel(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWithProvider(0, tm)

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

func TestAdvanceTime(t *testing.T) {
	mc := gomock.NewController(t)
	tm := mock.NewMockTimeProvider(mc)

	sched.DefaultScheduler = sched.NewWithProvider(0, tm)

	start := time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC)
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
}

type Job struct {
	sched.Job
	JobFn func()
}

func ExpectJobs(t *testing.T, expected ...Job) {
	require.Equal(t, len(expected), sched.Len())

	var actual []Job
	ok := sched.Scan(sched.Job{}, func(j sched.Job, jobFn func()) bool {
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

func EqualFn(t *testing.T, e, a func(), msgAndArgs ...interface{}) {
	fe := runtime.FuncForPC(reflect.ValueOf(e).Pointer()).Name()
	fa := runtime.FuncForPC(reflect.ValueOf(a).Pointer()).Name()
	require.Equal(t, fe, fa, msgAndArgs...)
}
