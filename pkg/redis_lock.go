package pkg

import (
	"context"
	_ "embed"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"time"
)

var (
	//go:embed lua/unlock.lua
	luaUnlock string
	//go:embed lua/refresh.lua
	luaRefresh string
	//go:embed lua/lock.lua
	luaLock string
)

type Client struct {
	r      redis.Cmdable
	genVal func() string
}

func NewClient(r redis.Cmdable, ops ...ClientOption) *Client {
	res := &Client{
		r: r,
		genVal: func() string {
			return uuid.New().String()
		},
	}

	for _, op := range ops {
		op(res)
	}
	return res
}

func (c *Client) TryLock(ctx context.Context, key string, expiration time.Duration) (*Lock, error) {
	val := c.genVal()
	ok, err := c.r.SetNX(ctx, key, val, expiration).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrFailedToGetLock
	}

	return &Lock{
		rdb:        c.r,
		key:        key,
		val:        val,
		expiration: expiration,
	}, nil
}

func (c *Client) Lock(
	ctx context.Context,
	key string,
	expiration time.Duration,
	timeout time.Duration,
	retry RetryStrategy) (*Lock, error) {

	val := c.genVal()
	var timer *time.Timer

	for {
		//尝试获取锁
		lctx, cancel := context.WithTimeout(ctx, timeout)
		res, err := c.r.Eval(lctx, luaLock, []string{key}, val, expiration.Seconds()).Result()
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if res == "OK" {
			return &Lock{
				rdb:        c.r,
				key:        key,
				val:        val,
				expiration: expiration,
			}, nil
		}

		//如果获取失败，询问是否重试
		interval, re := retry.Next()
		if !re {
			return nil, ErrFailedToGetLock
		}
		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}

		// 等待重试
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			continue
		}
	}
}

type Lock struct {
	rdb        redis.Cmdable
	key        string
	val        string
	expiration time.Duration
	unlockChan chan struct{}
}

func (l *Lock) Unlock(ctx context.Context) error {
	res, err := l.rdb.Eval(ctx, luaUnlock, []string{l.key}, l.val).Int64()
	if errors.Is(err, redis.Nil) {
		return ErrLockNotHold
	}
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHold
	}
	close(l.unlockChan)
	return nil
}

func (l *Lock) Refresh(ctx context.Context) error {
	res, err := l.rdb.Eval(ctx, luaRefresh, []string{l.key}, l.val, l.expiration.Seconds()).Int64()
	if errors.Is(err, redis.Nil) {
		return ErrLockNotHold
	}
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHold
	}
	return nil
}

// AutoRefresh
// @param interval is the interval between two refresh
// @param timeout is the timeout of refresh
// @return error when AutoRefresh is stopped
func (l *Lock) AutoRefresh(ctx context.Context, interval time.Duration, timeout time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			childCtx, _ := context.WithTimeout(ctx, timeout)
			if err := l.Refresh(childCtx); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-l.unlockChan:
			return nil
		}
	}
}
