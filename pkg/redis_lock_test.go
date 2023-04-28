package pkg

import (
	"context"
	"github.com/Handkerchiefs-t/distributed_lock/pkg/mocks"
	"github.com/golang/mock/gomock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClient_TryLock(t *testing.T) {
	testCases := []struct {
		name string

		mock func(ctrl *gomock.Controller) *Client

		key        string
		expiration time.Duration

		wantLock *Lock
		wantErr  error
	}{
		{
			name: "set nx error",
			mock: func(ctrl *gomock.Controller) *Client {
				resp := redis.NewBoolResult(false, context.DeadlineExceeded)
				cmd := mocks.NewMockCmdable(ctrl)
				cmd.EXPECT().SetNX(context.Background(), "key", gomock.Any(), time.Second).Return(resp)
				return NewClient(cmd)
			},

			key:        "key",
			expiration: time.Second,
			wantErr:    context.DeadlineExceeded,
		},
		{
			name: "failed to get lock",
			mock: func(ctrl *gomock.Controller) *Client {
				resp := redis.NewBoolResult(false, nil)
				cmd := mocks.NewMockCmdable(ctrl)
				cmd.EXPECT().SetNX(context.Background(), "key", gomock.Any(), time.Second).Return(resp)
				return NewClient(cmd)
			},

			key:        "key",
			expiration: time.Second,
			wantErr:    ErrFailedToGetLock,
		},
		{
			name: "locked",
			mock: func(ctrl *gomock.Controller) *Client {
				resp := redis.NewBoolResult(true, nil)
				cmd := mocks.NewMockCmdable(ctrl)
				cmd.EXPECT().SetNX(context.Background(), "key", gomock.Any(), time.Second).Return(resp)
				return NewClient(cmd)
			},

			key:        "key",
			expiration: time.Second,
			wantLock: &Lock{
				key:        "key",
				expiration: time.Second,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			client := tc.mock(ctrl)
			lock, err := client.TryLock(context.Background(), tc.key, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantLock.key, lock.key)
			assert.Equal(t, tc.wantLock.expiration, lock.expiration)
			assert.NotEmpty(t, lock.val)
		})
	}
}

func TestLock_Unlock(t *testing.T) {
	testCases := []struct {
		name string

		mock func(ctrl *gomock.Controller) *Lock

		wantErr error
	}{
		{
			name: "normal unlocked",

			mock: func(ctrl *gomock.Controller) *Lock {
				resp := redis.NewCmd(context.Background())
				resp.SetVal(int64(1))
				cmd := mocks.NewMockCmdable(ctrl)
				cmd.EXPECT().
					Eval(context.Background(), luaUnlock, []string{"key"}, []any{"uuid"}).
					Return(resp)
				return &Lock{
					client: NewClient(cmd),
					key:    "key",
					val:    "uuid",
				}
			},

			wantErr: nil,
		},
		{
			name: "not hold lock",

			mock: func(ctrl *gomock.Controller) *Lock {
				resp := redis.NewCmd(context.Background())
				// means not hold lock
				resp.SetVal(int64(0))
				cmd := mocks.NewMockCmdable(ctrl)
				cmd.EXPECT().
					Eval(context.Background(), luaUnlock, []string{"key"}, []any{"uuid"}).
					Return(resp)
				return &Lock{
					client: NewClient(cmd),
					key:    "key",
					val:    "uuid",
				}
			},

			wantErr: ErrLockNotHold,
		},
		{
			name: "redis error",

			mock: func(ctrl *gomock.Controller) *Lock {
				resp := redis.NewCmd(context.Background())
				resp.SetErr(context.DeadlineExceeded)
				cmd := mocks.NewMockCmdable(ctrl)
				cmd.EXPECT().
					Eval(context.Background(), luaUnlock, []string{"key"}, []any{"uuid"}).
					Return(resp)
				return &Lock{
					client: NewClient(cmd),
					key:    "key",
					val:    "uuid",
				}
			},

			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			lock := tc.mock(ctrl)
			err := lock.Unlock(context.Background())
			assert.Equal(t, tc.wantErr, err)
		})
	}

}

func TestLock_Refresh(t *testing.T) {
	testCases := []struct {
		name string

		mock func(ctrl *gomock.Controller) *Lock

		wantErr error
	}{
		{
			name: "normal refreshed",

			mock: func(ctrl *gomock.Controller) *Lock {
				resp := redis.NewCmd(context.Background())
				resp.SetVal(int64(1))
				cmd := mocks.NewMockCmdable(ctrl)
				cmd.EXPECT().
					Eval(context.Background(), luaRefresh, []string{"key"}, []any{"uuid", float64(60)}).
					Return(resp)
				return &Lock{
					client:     NewClient(cmd),
					key:        "key",
					val:        "uuid",
					expiration: time.Minute,
				}
			},

			wantErr: nil,
		},
		{
			name: "not hold lock",

			mock: func(ctrl *gomock.Controller) *Lock {
				resp := redis.NewCmd(context.Background())
				// means not hold lock
				resp.SetVal(int64(0))
				cmd := mocks.NewMockCmdable(ctrl)
				cmd.EXPECT().
					Eval(context.Background(), luaRefresh, []string{"key"}, []any{"uuid", float64(60)}).
					Return(resp)
				return &Lock{
					client:     NewClient(cmd),
					key:        "key",
					val:        "uuid",
					expiration: time.Minute,
				}
			},

			wantErr: ErrLockNotHold,
		},
		{
			name: "redis error",

			mock: func(ctrl *gomock.Controller) *Lock {
				resp := redis.NewCmd(context.Background())
				resp.SetErr(context.DeadlineExceeded)
				cmd := mocks.NewMockCmdable(ctrl)
				cmd.EXPECT().
					Eval(context.Background(), luaRefresh, []string{"key"}, []any{"uuid", float64(60)}).
					Return(resp)
				return &Lock{
					client:     NewClient(cmd),
					key:        "key",
					val:        "uuid",
					expiration: time.Minute,
				}
			},

			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			lock := tc.mock(ctrl)
			err := lock.Refresh(context.Background())
			assert.Equal(t, tc.wantErr, err)
		})
	}

}
