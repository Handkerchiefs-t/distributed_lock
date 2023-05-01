package pkg

import (
	"time"
)

type ClientOption func(client *Client)

func UseGenValueFunc(f func() string) ClientOption {
	return func(l *Client) {
		l.genVal = f
	}
}

type RetryStrategy interface {
	Next() (time.Duration, bool)
}

type FixedRetryStrategy struct {
	Interval time.Duration
	MaxCount int
	count    int
}

func (f *FixedRetryStrategy) Next() (time.Duration, bool) {
	if f.count >= f.MaxCount {
		return 0, false
	}
	f.count++
	return f.Interval, true
}
