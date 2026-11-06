// Package storetest gives integration tests a real Redis connection and a
// key namespace of their own, so tests in different packages can run at the
// same time against one Redis server.
package storetest

import (
	"context"
	"crypto/rand"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/config"
	"github.com/sham-33/job-scheduler/internal/store"
)

// DB keeps tests away from the data a local worker uses in DB 0.
const DB = 15

// Client connects to REDIS_TEST_ADDR (default localhost:6379) and skips the
// test when Redis isn't reachable.
func Client(t testing.TB) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb, err := store.Connect(context.Background(), config.Config{RedisAddr: addr, RedisDB: DB})
	if err != nil {
		t.Skipf("Redis not reachable at %s (start it with `make redis-up`): %v", addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// Keys returns a namespace no other test uses and deletes everything in it
// when the test ends.
func Keys(t testing.TB, rdb *redis.Client) store.Keys {
	t.Helper()
	prefix := "test:" + rand.Text()[:12] + ":"
	t.Cleanup(func() {
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, prefix+"*", 1000).Iterator()
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
		}
	})
	return store.NewKeys(prefix)
}
