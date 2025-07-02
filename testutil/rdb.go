package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

// NewRedisClient returns a new redis client and in-memory server.
// It registers a cleanup of redis data after each test.
func NewRedisClientWithCleanup(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	server := miniredis.RunT(t)
	rsClient := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status := rsClient.Ping(ctx)
	if err := status.Err(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	return rsClient, server
}
