//go:build ignore
// +build ignore

package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/goforj/scheduler"
)

func main() {
	// NewRedisLocker creates a RedisLocker with a client and TTL.

	// Example: create a redis-backed locker
	client := redis.NewClient(&redis.Options{}) // replace with your client
	locker := scheduler.NewRedisLocker(client, time.Minute)
	_, _ = locker.Lock(context.Background(), "job")
}
