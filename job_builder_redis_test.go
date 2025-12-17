package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type mockRedisClient struct {
	setNXResult bool
	setNXErr    error
	delErr      error
	delCalls    int
}

func (m *mockRedisClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return redis.NewBoolResult(m.setNXResult, m.setNXErr)
}

func (m *mockRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	m.delCalls++
	return redis.NewIntResult(int64(len(keys)), m.delErr)
}

func TestRedisLockerSuccess(t *testing.T) {
	client := &mockRedisClient{setNXResult: true}
	locker := NewRedisLocker(client, time.Minute)

	lock, err := locker.Lock(context.Background(), "job1")
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.NoError(t, lock.Unlock(context.Background()))
	require.Equal(t, 1, client.delCalls)
}

func TestRedisLockerNotAcquired(t *testing.T) {
	client := &mockRedisClient{setNXResult: false}
	locker := NewRedisLocker(client, time.Minute)

	_, err := locker.Lock(context.Background(), "job1")
	require.ErrorIs(t, err, errLockNotAcquired)
}

func TestRedisLockerSetNXError(t *testing.T) {
	client := &mockRedisClient{setNXErr: errors.New("boom")}
	locker := NewRedisLocker(client, time.Minute)

	_, err := locker.Lock(context.Background(), "job1")
	require.EqualError(t, err, "boom")
}
