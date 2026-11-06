package store

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sham-33/job-scheduler/internal/config"
)

// testDB keeps tests away from the data a local worker uses in DB 0.
const testDB = 15

func testAddr() string {
	if addr := os.Getenv("REDIS_TEST_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func TestConnect(t *testing.T) {
	rdb, err := Connect(context.Background(), config.Config{RedisAddr: testAddr(), RedisDB: testDB})
	if err != nil {
		t.Skipf("Redis not reachable at %s (start it with `make redis-up`): %v", testAddr(), err)
	}
	defer rdb.Close()

	if got := rdb.Options().DB; got != testDB {
		t.Errorf("client DB = %d, want %d", got, testDB)
	}
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Errorf("Ping after Connect: %v", err)
	}
}

func TestConnectUnreachable(t *testing.T) {
	const addr = "127.0.0.1:1" // nothing listens on port 1
	_, err := Connect(context.Background(), config.Config{RedisAddr: addr})
	if err == nil {
		t.Fatal("Connect succeeded, want error")
	}
	if !strings.Contains(err.Error(), addr) {
		t.Errorf("error %q does not name the address %s", err, addr)
	}
}
