//go:build e2e

package pkg

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClient_e2e_TryLock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	client := NewClient(rdb)

	testCases := []struct {
		name string

		key        string
		val        string
		expiration time.Duration

		ctx    func() context.Context
		before func(t *testing.T)
		after  func(t *testing.T)

		wantErr  error
		wantLock *Lock
	}{
		{
			name: "locked",

			ctx: func() context.Context {
				res, _ := context.WithTimeout(context.Background(), time.Second*3)
				return res
			},
			before: func(t *testing.T) {},
			after: func(t *testing.T) {
				val, err := rdb.GetDel(context.Background(), "key1").Result()
				assert.NoError(t, err)
				assert.NotEmpty(t, val)
			},

			key:        "key1",
			expiration: time.Minute,

			wantErr: nil,
			wantLock: &Lock{
				client: client,
				key:    "key1",
			},
		},
		{
			name: "lock failed",

			ctx: func() context.Context {
				res, _ := context.WithTimeout(context.Background(), time.Second*3)
				return res
			},
			before: func(t *testing.T) {
				rsp, err := rdb.Set(context.Background(), "key2", "value2", time.Minute).Result()
				t.Logf("before: %s", rsp)
				assert.NoError(t, err)
				assert.NotEmpty(t, rsp)
			},
			after: func(t *testing.T) {
				val, err := rdb.GetDel(context.Background(), "key2").Result()
				t.Logf("after: %s", val)
				assert.NoError(t, err)
				assert.NotEmpty(t, val)
			},

			key:        "key2",
			expiration: time.Minute,

			wantErr: ErrFailedToGetLock,
		},
		{
			name: "lock error",

			ctx: func() context.Context {
				res, cancel := context.WithTimeout(context.Background(), time.Second*3)
				cancel()
				return res
			},
			before: func(t *testing.T) {},
			after:  func(t *testing.T) {},

			key:        "key3",
			expiration: time.Minute,

			wantErr: context.Canceled,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			defer tc.after(t)

			ctx := tc.ctx()
			lock, err := client.TryLock(ctx, tc.key, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantLock.key, lock.key)
			assert.NotEmpty(t, lock.val)
			assert.NotNil(t, lock.client)
		})
	}
}

func TestLock_e2e_UnLock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	client := NewClient(rdb)

	testCases := []struct {
		name string

		ctx    func() context.Context
		before func(t *testing.T)
		after  func(t *testing.T)

		key string
		val string

		wantErr error
	}{
		{
			name: "unlock success",

			ctx: func() context.Context { return context.Background() },
			before: func(t *testing.T) {
				rsp, err := rdb.Set(context.Background(), "key1", "value1", time.Minute).Result()
				t.Logf("before: %s", rsp)
				assert.NoError(t, err)
				assert.NotEmpty(t, rsp)
			},
			after: func(t *testing.T) {
				exist, err := rdb.Exists(context.Background(), "key1").Result()
				assert.NoError(t, err)
				assert.Equal(t, int64(0), exist)
			},

			key: "key1",
			val: "value1",

			wantErr: nil,
		},
		{
			name: "not hold lock",

			ctx:    func() context.Context { return context.Background() },
			before: func(t *testing.T) {},
			after:  func(t *testing.T) {},

			key: "key2",
			val: "value2",

			wantErr: ErrLockNotHold,
		},
		{
			name: "hold by other",

			ctx: func() context.Context { return context.Background() },
			before: func(t *testing.T) {
				rsp, err := rdb.Set(context.Background(), "key3", "value3", time.Minute).Result()
				t.Logf("before: %s", rsp)
				assert.NoError(t, err)
				assert.NotEmpty(t, rsp)
			},
			after: func(t *testing.T) {
				val, err := rdb.Get(context.Background(), "key3").Result()
				t.Logf("after: %s", val)
				assert.NoError(t, err)
				assert.NotEmpty(t, val)
			},

			key: "key3",
			val: "value3_not_hold",

			wantErr: ErrLockNotHold,
		},
		{
			name: "error",

			ctx: func() context.Context {
				res, cancel := context.WithTimeout(context.Background(), time.Second*3)
				cancel()
				return res
			},
			before: func(t *testing.T) {},
			after:  func(t *testing.T) {},

			key: "key4",
			val: "value4",

			wantErr: context.Canceled,
		},
	}

	for _, ts := range testCases {
		t.Run(ts.name, func(t *testing.T) {
			ts.before(t)
			defer ts.after(t)

			lock := &Lock{
				client: client,
				key:    ts.key,
				val:    ts.val,
			}
			err := lock.Unlock(ts.ctx())
			assert.Equal(t, ts.wantErr, err)
		})
	}
}
