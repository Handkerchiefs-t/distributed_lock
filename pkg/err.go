package pkg

import "fmt"

var (
	ErrFailedToGetLock = fmt.Errorf("redis log: failed to get lock")
	ErrLockNotHold     = fmt.Errorf("redis log: you don't hold the lock")
)
