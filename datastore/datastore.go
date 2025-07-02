// The datastore package provides a simple abstraction over Redis for storing and retrieving data.
package datastore

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v8"
	"github.com/holmberd/go-entitystore/keyfactory"
)

var (
	ErrKeyNotFound = errors.New("datastore: key not found")
)

// Client represents a datastore client for interacting with a datastore.
// The client is safe for concurrent use.
type Client struct {
	rsClient *redis.Client
}

// NewClient creates a new instance of a Client.
func NewClient(rsClient *redis.Client) (*Client, error) {
	return &Client{
		rsClient: rsClient,
	}, nil
}

// GetRSClient returns the underlying Redis client.
//
// NOTE: This is an escape mechanism and should not be abused.
func (c *Client) GetRSClient() *redis.Client {
	return c.rsClient
}

// Put writes the data with the key to the store.
// If the key doesn't exist it's added, otherwise it's updated.
func (c *Client) Put(
	ctx context.Context,
	key *keyfactory.Key,
	data []byte,
	expiration time.Duration,
) error {
	if key == nil {
		return nil // No-op for empty key.
	}
	err := c.rsClient.Set(ctx, key.RedisKey(), data, expiration).Err()
	if err != nil {
		return fmt.Errorf("datastore: failed to write key '%s': %w", key, err)
	}
	return nil
}

// PutMulti is a batch version of Put.
func (c *Client) PutMulti(
	ctx context.Context,
	keys []*keyfactory.Key,
	data [][]byte,
	expiration time.Duration,
) error {
	if len(keys) != len(data) {
		return errors.New("datastore: key and data slices have different length")
	}
	if len(keys) == 0 {
		return nil // No-op for empty batch.
	}

	// Use a map to store key-value pairs as expected by redis MSet.
	kvPairs := make(map[string]interface{}, len(keys))
	for i, key := range keys {
		kvPairs[key.RedisKey()] = data[i]
	}

	pipe := c.rsClient.Pipeline()
	if err := pipe.MSet(ctx, kvPairs).Err(); err != nil {
		return fmt.Errorf("datastore: %w", err)
	}
	if expiration != 0 {
		// Set TTL per key.
		for key := range kvPairs {
			if err := pipe.Expire(ctx, key, expiration).Err(); err != nil {
				return fmt.Errorf("datastore: %w", err)
			}
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("datastore: failed to write keys: %w", err)
	}
	return nil
}

// Delete deletes the provided keys from the store.
func (c *Client) Delete(ctx context.Context, keys ...*keyfactory.Key) error {
	if len(keys) == 0 {
		return nil // No-op for empty keys.
	}
	rsKeys := make([]string, len(keys))
	for i, key := range keys {
		rsKeys[i] = key.RedisKey()
	}
	if err := c.rsClient.Del(ctx, rsKeys...).Err(); err != nil {
		return fmt.Errorf("datastore: failed to delete keys from redis: %w", err)
	}
	return nil
}

// DeleteMatch deletes all keys matching the key pattern.
//
// NOTE: This is a blocking operation.
func (c *Client) DeleteMatch(ctx context.Context, keyMatch *keyfactory.Key) error {
	if keyMatch == nil {
		return nil // No-op for empty key.
	}
	keys, err := c.GetKeys(ctx, keyMatch)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil // No-op.
	}
	return c.Delete(ctx, keys...)
}

// Get retrieves the data associated with the key from the store.
// ErrKeyNotFound is returned if the key is not found in the store.
func (c *Client) Get(ctx context.Context, key *keyfactory.Key) ([]byte, error) {
	if key == nil {
		return nil, nil // No-op for empty key.
	}
	data, err := c.rsClient.Get(ctx, key.RedisKey()).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("datastore: %w", err)
	}
	return data, nil
}

// GetMulti retrieves data by their associated keys from the store.
// If the key is not found in the store it is ignored and not included in the returned data slice.
func (c *Client) GetMulti(ctx context.Context, keys []*keyfactory.Key) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil // No-op for empty slice of keys.
	}
	rsKeys := make([]string, len(keys))
	for i, key := range keys {
		rsKeys[i] = key.RedisKey()
	}
	results, err := c.rsClient.MGet(ctx, rsKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("datastore: failed to retrieve keys: %w", err)
	}
	dataSlice := make([][]byte, 0, len(results))
	for _, res := range results {
		if res == nil {
			continue // Key not found; skip it.
		}
		// Convert result to expected redis string before converting to byte array.
		data, ok := res.(string)
		if !ok {
			// This should never occur since MGET should fail and return a command-level error.
			panic(fmt.Sprintf("datastore: unexpected type %T in redis MGET result", res))
		}
		// Optimzation: Since '[]byte(data)' result in copying the data string.
		// Instead we unsafe convert the string to []byte without copying.
		// Only safe if the caller does not modify the byte slice which now points to
		// an immutable string memory address.
		dataSlice = append(dataSlice, unsafe.Slice(unsafe.StringData(data), len(data)))
	}
	return dataSlice, nil
}

// GetKeysWithCursor retrieves all matching keys using cursor pagination.
//   - Does not gurantee an exact number of keys returned per page.
//   - A given key may be returned multiple times.
//   - Keys that were not constantly present in the collection during a full iteration, may be returned or not.
func (c *Client) GetKeysWithCursor(
	ctx context.Context,
	cursor uint64,
	limit int,
	keyMatch *keyfactory.Key,
) (keys []*keyfactory.Key, nextCursor uint64, err error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	// The Redis SCAN command only offer limited guarantees about the exact number of keys per call.
	// As a result, the exact batch size in each iteration is not guranteed.
	rsKeys, nextCursor, err := c.rsClient.Scan(
		ctx,
		cursor,
		keyMatch.RedisKey(),
		int64(limit),
	).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("datastore: failed scanning redis for keys: %w", err)
	}

	// Parse and convert redis keys to keys.
	keys = make([]*keyfactory.Key, len(rsKeys))
	var key *keyfactory.Key
	for i, rsKey := range rsKeys {
		key, err = keyfactory.ParseRedisKey(rsKey)
		if err != nil {
			return nil, 0, fmt.Errorf("datastore: failed to parse redis key: %w", err)
		}
		keys[i] = key
	}
	return keys, nextCursor, nil
}

// ScanKeys retrieves all matching keys as a non-blocking operation.
// Safe for production use, but may miss keys added/removed during iteration.
func (c *Client) ScanKeys(ctx context.Context, keyMatch *keyfactory.Key) ([]*keyfactory.Key, error) {
	cursor := uint64(0)
	limit := 1000 // Max limit.
	var allKeys []*keyfactory.Key
	for {
		keys, nextCursor, err := c.GetKeysWithCursor(ctx, cursor, limit, keyMatch)
		if err != nil {
			return nil, fmt.Errorf("datastore: %w", err)
		}
		allKeys = append(allKeys, keys...)
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}

	// Remove any potential duplicate keys returned during the scan.
	seen := make(map[string]struct{})
	keys := make([]*keyfactory.Key, 0, len(allKeys))
	for _, k := range allKeys {
		if _, exists := seen[k.RedisKey()]; !exists {
			seen[k.RedisKey()] = struct{}{}
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// GetKeys retrieves all matching keys.
//
// NOTE: This is a blocking operation.
func (c *Client) GetKeys(ctx context.Context, keyMatch *keyfactory.Key) ([]*keyfactory.Key, error) {
	rsKeys, err := c.rsClient.Keys(ctx, keyMatch.RedisKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("datastore: failed to retrieve keys from redis: %w", err)
	}

	// Parse and convert redis keys to keys.
	keys := make([]*keyfactory.Key, len(rsKeys))
	var key *keyfactory.Key
	for i, rsKey := range rsKeys {
		key, err = keyfactory.ParseRedisKey(rsKey)
		if err != nil {
			return nil, fmt.Errorf("datastore: failed to parse redis key: %w", err)
		}
		keys[i] = key
	}
	return keys, nil
}

// Exists checks whether the key exist in the store.
func (c *Client) Exists(ctx context.Context, key *keyfactory.Key) (bool, error) {
	if key == nil {
		return false, nil // No-op for empty key.
	}
	exists, err := c.rsClient.Exists(ctx, key.RedisKey()).Result()
	if err != nil {
		return false, fmt.Errorf("datastore: %w", err)
	}
	// Convert int64 to bool (1 = true, 0 = false).
	return exists > 0, nil
}
