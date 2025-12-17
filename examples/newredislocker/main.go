//go:build ignore
// +build ignore

package main

import (
	"context"
	"github.com/goforj/scheduler"
	"github.com/redis/go-redis/v9"
	"time"
)

func main() {
	// NewRedisLocker creates a RedisLocker with a client and TTL.

	// Example: create a redis-backed locker
	client := redis.NewClient(&redis.Options{}) // replace with your client
	locker := scheduler.NewRedisLocker(client, time.Minute)
	_, _ = locker.Lock(context.Background(), "job")
}
