package sched

import "time"

type timeProvider struct{}

func (p timeProvider) Now() Time {
	return time.Now()
}

func (p timeProvider) AfterFunc(d Duration, fn func()) Timer {
	return time.AfterFunc(d, fn)
}
