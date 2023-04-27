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
)

type Client struct {
	r redis.Cmdable
}

func (c *Client) TryLock(ctx context.Context, key string, expiration time.Duration) (*Lock, error) {
	uuid := uuid.New().String()
	ok, err := c.r.SetNX(ctx, key, uuid, expiration).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrFailedToGetLock
	}

	return &Lock{
		client: c,
		key:    key,
		uuid:   uuid,
	}, nil
}

type Lock struct {
	client *Client
	key    string
	uuid   string
}

func (l *Lock) Unlock(ctx context.Context) error {
	res, err := l.client.r.Eval(ctx, luaUnlock, []string{l.key}, l.uuid).Int64()
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
