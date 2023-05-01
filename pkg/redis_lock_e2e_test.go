//go:build e2e

package pkg

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClient_e2e_Lock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	vals := []string{}

	testCases := []struct {
		name string

		key        string
		val        string
		expiration time.Duration
		timeout    time.Duration

		ctx    func() context.Context
		before func(t *testing.T)
		after  func(t *testing.T)

		wantErr error
	}{
		{
			name: "lock success",

			key:        "key1",
			val:        "value1",
			expiration: time.Minute,
			timeout:    time.Minute,

			ctx:    func() context.Context { return context.Background() },
			before: func(t *testing.T) {},
			after: func(t *testing.T) {
				ttl, err := rdb.TTL(context.Background(), "key1").Result()
				assert.NoError(t, err)
				assert.True(t, ttl.Seconds() >= 50)
				val, err := rdb.GetDel(context.Background(), "key1").Result()
				assert.NoError(t, err)
				assert.Equal(t, "value1", val)
			},
		},
		{
			name: "not get lock",

			key:        "key2",
			val:        "value2",
			expiration: time.Minute,
			timeout:    time.Minute,

			ctx: func() context.Context { return context.Background() },
			before: func(t *testing.T) {
				resp, err := rdb.Set(context.Background(), "key2", "other value", time.Minute).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", resp)
			},
			after: func(t *testing.T) {
				val, err := rdb.GetDel(context.Background(), "key2").Result()
				assert.NoError(t, err)
				assert.Equal(t, "other value", val)
			},

			wantErr: ErrFailedToGetLock,
		},
		{
			name: "acquiring lock after retrying",

			key:        "key3",
			val:        "value3",
			expiration: time.Minute,
			timeout:    time.Minute,

			ctx: func() context.Context { return context.Background() },
			before: func(t *testing.T) {
				resp, err := rdb.Set(context.Background(), "key3", "other value", time.Second*2).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", resp)
			},
			after: func(t *testing.T) {
				ttl, err := rdb.TTL(context.Background(), "key3").Result()
				assert.NoError(t, err)
				assert.True(t, ttl.Seconds() >= 58)
				val, err := rdb.GetDel(context.Background(), "key3").Result()
				assert.NoError(t, err)
				assert.Equal(t, "value3", val)
			},
		},
		{
			name: "context timeout",

			key:        "key4",
			val:        "value4",
			expiration: time.Minute,
			timeout:    time.Minute,

			ctx: func() context.Context {
				res, _ := context.WithTimeout(context.Background(), time.Second)
				time.Sleep(time.Second * 2)
				return res
			},
			before: func(t *testing.T) {},
			after:  func(t *testing.T) {},

			wantErr: context.DeadlineExceeded,
		},
		{
			name: "context cancel",

			key:        "key5",
			val:        "value5",
			expiration: time.Minute,
			timeout:    time.Minute,

			ctx: func() context.Context {
				res, cancel := context.WithTimeout(context.Background(), time.Second)
				cancel()
				return res
			},
			before: func(t *testing.T) {},
			after:  func(t *testing.T) {},

			wantErr: context.Canceled,
		},
	}

	for _, tt := range testCases {
		vals = append(vals, tt.val)
	}
	genVal := func() string {
		res := vals[0]
		vals = vals[1:]
		return res
	}
	client := NewClient(rdb, UseGenValueFunc(genVal))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			defer tc.after(t)
			_, err := client.Lock(
				tc.ctx(),
				tc.key,
				tc.expiration,
				tc.timeout,
				&FixedRetryStrategy{
					MaxCount: 3,
					Interval: time.Second,
				},
			)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

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
				rdb:        client.r,
				key:        "key1",
				expiration: time.Minute,
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
			assert.Equal(t, tc.wantLock.expiration, lock.expiration)
			assert.NotEmpty(t, lock.val)
			assert.NotNil(t, lock.rdb)
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
				rdb: client.r,
				key: ts.key,
				val: ts.val,
			}
			err := lock.Unlock(ts.ctx())
			assert.Equal(t, ts.wantErr, err)
		})
	}
}

func TestLock_e2e_Refresh(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	client := NewClient(rdb)

	testCases := []struct {
		name string

		ctx    func() context.Context
		before func(t *testing.T)
		after  func(t *testing.T)

		key        string
		val        string
		expiration time.Duration

		wantErr error
	}{
		{
			name: "refresh success",

			ctx: func() context.Context { return context.Background() },
			before: func(t *testing.T) {
				rsp, err := rdb.Set(context.Background(), "refresh_key1", "value1", time.Second*10).Result()
				t.Logf("before: %s", rsp)
				assert.NoError(t, err)
				assert.NotEmpty(t, rsp)
			},
			after: func(t *testing.T) {
				result, err := rdb.TTL(context.Background(), "refresh_key1").Result()
				assert.NoError(t, err)
				assert.True(t, result > time.Second*10)
				t.Logf("refresh success after: %s", result)
			},

			key:        "refresh_key1",
			val:        "value1",
			expiration: time.Minute,

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
				rdb:        client.r,
				key:        ts.key,
				val:        ts.val,
				expiration: ts.expiration,
			}
			err := lock.Refresh(ts.ctx())
			assert.Equal(t, ts.wantErr, err)
		})
	}
}
