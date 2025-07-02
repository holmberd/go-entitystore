package datastore

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/holmberd/go-entitystore/keyfactory"
	"github.com/holmberd/go-entitystore/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDSClient(
	t *testing.T,
	rsClient *redis.Client,
) (*Client, context.Context, *keyfactory.KeyBuilderWithNamespace) {
	t.Helper()
	ctx := context.Background()

	// A random key is used as the key namespace to ensure test data isolation.
	keyNamespace := keyfactory.GenerateRandomKey()
	kb := keyfactory.NewKeyBuilderWithNamespace(keyNamespace)

	ds, err := NewClient(rsClient)
	require.NoError(t, err)

	t.Cleanup(func() {
		kb.Reset() // Reset builder after each test.

		// Flush data in store after each test where the parent function is called.
		kb := keyfactory.NewKeyBuilderWithNamespace(keyNamespace)
		kb.WithWildcard(keyfactory.WildcardAnyString)
		keyMatch, err := kb.BuildAndReset()
		if err != nil {
			t.Fatal(err)
		}
		err = ds.DeleteMatch(ctx, keyMatch)
		if err != nil {
			t.Fatalf("failed to flush datastore: %v", err)
		}
	})
	return ds, ctx, kb
}

func TestDatastoreClient(t *testing.T) {
	rsClient, server := testutil.NewRedisClientWithCleanup(t)
	defer server.Close()

	t.Run("Put and Get", func(t *testing.T) {
		ds, ctx, kb := setupDSClient(t, rsClient)
		kb.WithKey("put")
		key, err := kb.Build()
		assert.NoError(t, err)

		data := []byte("value")
		assert.NoError(t, ds.Put(ctx, key, data, 0))

		got, err := ds.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, data, got)
	})

	t.Run("PutMulti and GetMulti", func(t *testing.T) {
		keyPrefix := "item"
		numKeys := 3
		ds, ctx, kb := setupDSClient(t, rsClient)
		keys := make([]*keyfactory.Key, numKeys)
		data := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
		var k *keyfactory.Key
		var err error
		for i := range numKeys {
			kb.WithKey(fmt.Sprintf("%s-%d", keyPrefix, i))
			k, err = kb.Build()
			assert.NoError(t, err)
			keys[i] = k
		}
		assert.NoError(t, ds.PutMulti(ctx, keys, data, 0))

		got, err := ds.GetMulti(ctx, keys)
		assert.NoError(t, err)
		assert.Len(t, got, numKeys)
		assert.Equal(t, data[0], got[0])
		assert.Equal(t, data[1], got[1])
		assert.Equal(t, data[2], got[2])
	})

	t.Run("Delete and Exists", func(t *testing.T) {
		ds, ctx, kb := setupDSClient(t, rsClient)
		kb.WithKey("to-delete")
		key, err := kb.Build()
		assert.NoError(t, err)

		assert.NoError(t, ds.Put(ctx, key, []byte("temp"), 0))
		exists, err := ds.Exists(ctx, key)
		assert.NoError(t, err)
		assert.True(t, exists)

		assert.NoError(t, ds.Delete(ctx, key))
		exists, err = ds.Exists(ctx, key)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("DeleteMulti", func(t *testing.T) {
		parentKey := "delete"
		numKeys := 3
		ds, ctx, kb := setupDSClient(t, rsClient)

		// Add keys.
		keys := make([]*keyfactory.Key, 0, numKeys)
		data := make([][]byte, 0, numKeys)
		for i := range numKeys {
			kb.WithParentKey(parentKey)
			kb.WithKey(fmt.Sprint(i))
			key, err := kb.BuildAndReset()
			assert.NoError(t, err)
			keys = append(keys, key)
			data = append(data, []byte("val"))
		}
		assert.NoError(t, ds.PutMulti(ctx, keys, data, 0))

		// Assert keys exists.
		kb.WithParentKey(parentKey)
		kb.WithWildcard(keyfactory.WildcardAnyString)
		keyMatch, err := kb.BuildAndReset()
		assert.NoError(t, err)
		foundKeys, err := ds.GetKeys(ctx, keyMatch)
		assert.NoError(t, err)
		assert.Len(t, foundKeys, 3)

		// Delete keys and assert.
		assert.NoError(t, ds.Delete(ctx, keys...))
		foundKeys, err = ds.GetKeys(ctx, keyMatch)
		assert.NoError(t, err)
		assert.Len(t, foundKeys, 0)
	})

	t.Run("DeleteMatch", func(t *testing.T) {
		parentKey := "delete"
		ds, ctx, kb := setupDSClient(t, rsClient)

		// Add keys and ensure they exists.
		kb.WithParentKey(parentKey)
		kb.WithKey("one")
		key, err := kb.BuildAndReset()
		assert.NoError(t, err)
		assert.NoError(t, ds.Put(ctx, key, []byte("temp"), 0))
		exists, err := ds.Exists(ctx, key)
		assert.NoError(t, err)
		assert.True(t, exists)

		// Delete keys and ensure they are deleted.
		kb.WithParentKey(parentKey)
		kb.WithWildcard(keyfactory.WildcardAnyString)
		keyMatch, err := kb.BuildAndReset()
		assert.NoError(t, err)
		assert.NoError(t, ds.DeleteMatch(ctx, keyMatch))
		exists, err = ds.Exists(ctx, key)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("GetKeysWithCursor", func(t *testing.T) {
		ds, ctx, kb := setupDSClient(t, rsClient)
		numKeys := 25
		prefix := "cursor-key"
		keys := make([]*keyfactory.Key, 0, numKeys)
		data := make([][]byte, 0, numKeys)
		for i := range numKeys {
			kb.WithParentKey(prefix)
			kb.WithKey(fmt.Sprint(i))
			key, err := kb.Build()
			assert.NoError(t, err)
			keys = append(keys, key)
			data = append(data, []byte("val"))
		}
		assert.NoError(t, ds.PutMulti(ctx, keys, data, 0))

		kb.Reset()
		kb.WithParentKey(prefix)
		kb.WithWildcard(keyfactory.WildcardAnyString)
		keyMatch, err := kb.Build()
		assert.NoError(t, err)

		cursor := uint64(0)
		limit := 10
		var foundKeys []*keyfactory.Key
		for {
			keys, nextCursor, err := ds.GetKeysWithCursor(ctx, cursor, limit, keyMatch)
			assert.NoError(t, err)
			foundKeys = append(foundKeys, keys...)
			if nextCursor == 0 {
				break
			}
			cursor = nextCursor
		}

		// Remove any potential duplicate keys returned.
		seen := make(map[string]struct{})
		allKeys := make([]*keyfactory.Key, 0, len(foundKeys))
		for _, k := range foundKeys {
			if _, exists := seen[k.RedisKey()]; !exists {
				seen[k.RedisKey()] = struct{}{}
				allKeys = append(allKeys, k)
			}
		}
		assert.Len(t, allKeys, numKeys)
	})

	t.Run("ScanKeys", func(t *testing.T) {
		ds, ctx, kb := setupDSClient(t, rsClient)
		numKeys := 3
		parentKey := "scan-key"
		keys := make([]*keyfactory.Key, 0, numKeys)
		data := make([][]byte, 0, numKeys)
		for i := range numKeys {
			kb.WithParentKey(parentKey)
			kb.WithKey(fmt.Sprint(i))
			key, err := kb.Build()
			assert.NoError(t, err)
			keys = append(keys, key)
			data = append(data, []byte("val"))
		}
		assert.NoError(t, ds.PutMulti(ctx, keys, data, 0))

		kb.Reset()
		kb.WithParentKey(parentKey)
		kb.WithWildcard(keyfactory.WildcardAnyString)
		keyMatch, err := kb.Build()
		assert.NoError(t, err)
		foundKeys, err := ds.ScanKeys(ctx, keyMatch)
		assert.NoError(t, err)
		require.Len(t, foundKeys, numKeys)
	})

	t.Run("GetKeys", func(t *testing.T) {
		ds, ctx, kb := setupDSClient(t, rsClient)
		numKeys := 3
		parentKey := "scan-key"
		keys := make([]*keyfactory.Key, 0, numKeys)
		data := make([][]byte, 0, numKeys)
		for i := range numKeys {
			kb.WithParentKey(parentKey)
			kb.WithKey(fmt.Sprint(i))
			key, err := kb.Build()
			assert.NoError(t, err)
			keys = append(keys, key)
			data = append(data, []byte("val"))
		}
		assert.NoError(t, ds.PutMulti(ctx, keys, data, 0))

		kb.Reset()
		kb.WithParentKey(parentKey)
		kb.WithWildcard(keyfactory.WildcardAnyString)
		keyMatch, err := kb.Build()
		assert.NoError(t, err)

		foundKeys, err := ds.GetKeys(ctx, keyMatch)
		assert.NoError(t, err)
		require.Len(t, foundKeys, numKeys)
	})
}
