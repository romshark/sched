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
