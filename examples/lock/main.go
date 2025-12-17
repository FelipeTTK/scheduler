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
	// Lock obtains a lock for the job name.

	// Example: acquire a lock
	client := redis.NewClient(&redis.Options{})
	locker := scheduler.NewRedisLocker(client, time.Minute)
	lock, _ := locker.Lock(context.Background(), "job")
	_ = lock.Unlock(context.Background())
}
