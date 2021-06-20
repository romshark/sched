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

	tm.EXPECT().
		Now().
		MaxTimes(2).
		Return(time.Date(2021, 6, 20, 10, 00, 00, 0, time.UTC))

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

	require.Equal(t, 0, sched.Len())
	j, err := sched.Schedule(sched.Hour, callback)
	require.NoError(t, err)
	require.NotZero(t, j)

	ExpectJobs(t, Job{j, callback})
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
	i := 0
	ok := sched.Scan(sched.Job{}, func(j sched.Job, jobFn func()) bool {
		require.Equal(t, expected[i].Job, j)
		EqualFn(t, expected[i].JobFn, jobFn)
		return true
	})
	require.True(t, ok)
}

func EqualFn(t *testing.T, a, b func()) {
	f1 := runtime.FuncForPC(reflect.ValueOf(a).Pointer()).Name()
	f2 := runtime.FuncForPC(reflect.ValueOf(b).Pointer()).Name()
	require.Equal(t, f1, f2)
}
