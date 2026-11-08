package store

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

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

func TestPoolSize(t *testing.T) {
	for _, concurrency := range []int{1, 5, 25} {
		if got := poolSize(concurrency); got <= 2*concurrency {
			t.Errorf("poolSize(%d) = %d, want more than 2 per job slot", concurrency, got)
		}
	}
}

func TestConnect(t *testing.T) {
	rdb, err := Connect(context.Background(), config.Config{RedisAddr: testAddr(), RedisDB: testDB, Concurrency: 5})
	if err != nil {
		t.Skipf("Redis not reachable at %s (start it with `make redis-up`): %v", testAddr(), err)
	}
	defer rdb.Close()

	if got := rdb.Options().DB; got != testDB {
		t.Errorf("client DB = %d, want %d", got, testDB)
	}
	if got, want := rdb.Options().PoolSize, poolSize(5); got != want {
		t.Errorf("client PoolSize = %d, want %d", got, want)
	}
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Errorf("Ping after Connect: %v", err)
	}
}

// A server that accepts connections and then says nothing is what a hung
// Redis looks like. The caller's deadline has to win over go-redis's own 3s
// timeout, or a worker couldn't notice in time that its lease is at risk.
func TestConnectHonorsContextDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := Connect(ctx, config.Config{RedisAddr: ln.Addr().String()}); err == nil {
		t.Fatal("Connect succeeded against a silent server, want error")
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("Connect took %s to give up, want about the 100ms deadline", took)
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
