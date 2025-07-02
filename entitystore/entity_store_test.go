package entitystore

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/holmberd/go-entitystore/datastore"
	"github.com/holmberd/go-entitystore/keyfactory"
	"github.com/holmberd/go-entitystore/testutil"
	"github.com/stretchr/testify/assert"
)

type mockEntity struct {
	key string
	Id  string
}

func newMockEntity(id string) (*mockEntity, error) {
	key, err := keyfactory.NewEntityKey(keyfactory.EntityKindTest, id, "", "")
	if err != nil {
		return nil, err
	}
	return &mockEntity{
		key: key,
		Id:  id,
	}, nil
}

func (e mockEntity) GetKey() string {
	return e.key
}

func (e mockEntity) MarshalProto() ([]byte, error) {
	return []byte{}, nil
}

func (e *mockEntity) UnmarshalProto(data []byte) error {
	return nil
}

// setupMockEntityStore initializes a new store with test data isolation and cleanup.
func setupMockEntityStore(
	t *testing.T,
	rsClient *redis.Client,
) (*EntityStore[mockEntity, *mockEntity], context.Context) {
	t.Helper()
	ctx := context.Background()
	dsClient, err := datastore.NewClient(rsClient)
	if err != nil {
		t.Fatalf("failed to create datastore client: %v", err)
	}
	// Set random key as store key namespace to ensure test data isolation.
	store, err := New[mockEntity](
		string(keyfactory.EntityKindTest),
		keyfactory.GenerateRandomKey(),
		dsClient,
	)
	if err != nil {
		t.Fatalf("failed to create mock entity store: %v", err)
	}

	t.Cleanup(func() {
		// Flush the store data after each test.
		// TODO: Not necessary when using testutil.NewRedisClientWithCleanup.
		err := store.flush(ctx)
		if err != nil {
			t.Fatalf("failed to flush mock entity store: %v", err)
		}
	})
	return store, ctx
}

// Generic EntityStore tests.
func TestEntityStore(t *testing.T) {
	rsClient, server := testutil.NewRedisClientWithCleanup(t)
	defer server.Close()

	t.Run("Flush store", func(t *testing.T) {
		store1, ctx := setupMockEntityStore(t, rsClient)
		store2, _ := setupMockEntityStore(t, rsClient)
		entity, err := newMockEntity("me-1")
		assert.NoError(t, err)

		// Add entity to both stores.
		_, err = store1.Add(ctx, *entity, 0)
		assert.NoError(t, err)
		_, err = store2.Add(ctx, *entity, 0)
		assert.NoError(t, err)

		// Flush store1.
		err = store1.flush(ctx)
		assert.NoError(t, err)

		// Assert entity exist in store2.
		entities, err := store2.GetAll(ctx, "")
		assert.NoError(t, err)
		assert.Len(t, entities, 1)
	})

	t.Run("Add entity with invalid key", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		_, err := store.Add(ctx, mockEntity{}, 0)
		assert.Error(t, err, "should return an error when adding an entity with an invalid key")
	})

	t.Run("Add an empty entity batch", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		_, err := store.AddBatch(ctx, []mockEntity{}, 0)
		assert.NoError(t, err, "should not error when adding an empty batch")
	})

	t.Run("Add a nil entity batch", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		_, err := store.AddBatch(ctx, nil, 0)
		assert.NoError(t, err, "should not error when adding a nil batch")
	})

	t.Run("Add with invalid entity", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		invalidEntity := mockEntity{Id: "", key: ""}
		_, err := store.AddBatch(ctx, []mockEntity{invalidEntity}, 0)
		assert.Error(t, err, "should return error when adding a batch with invalid entity")
	})

	t.Run("Retrieve non-existent entity", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		entityOut, err := store.Get(ctx, "non-existent-key")
		assert.Error(t, err, "should return an error when retrieving a non-existent entity")
		assert.Nil(t, entityOut, "retrieved entity should be nil when not found")
	})

	t.Run("Retrieve entity with empty key", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		entityOut, err := store.Get(ctx, "")
		assert.NoError(t, err, "should not error when retrieving with an empty key")
		assert.Nil(t, entityOut, "retrieved entity should be nil for empty key")
	})

	t.Run("Retrieve non-existent entity", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		res, err := store.GetByKeys(ctx, []string{"non-existent-key"})
		assert.NoError(t, err, "should not error when retrieving entities with non-existent keys")
		assert.Len(t, res, 0)
	})

	t.Run("Retrieve entities with empty key list", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		res, err := store.GetByKeys(ctx, []string{})
		assert.NoError(t, err, "should not error when retrieving with an empty key list")
		var empty []*mockEntity
		assert.Equal(t, res, empty)
	})

	t.Run("Retrieve all entities from an empty store", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		entities, err := store.GetAll(ctx, "")
		assert.NoError(t, err, "should not error when fetching from an empty store")
		assert.Len(t, entities, 0, "should return no entities when store is empty")
	})

	t.Run("Remove a non-existent key", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		err := store.Remove(ctx, "non-existent-key")
		assert.NoError(t, err, "should not error when removing a non-existent entity")
	})

	t.Run("Remove with empty key", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		err := store.Remove(ctx, "")
		assert.NoError(t, err, "should not error when removing with an empty key")
	})

	t.Run("Remove an empty list of keys", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		err := store.RemoveByKeys(ctx, []string{})
		assert.NoError(t, err, "should not error when trying to remove an empty batch")
	})

	t.Run("Remove a nil list of keys", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		err := store.RemoveByKeys(ctx, nil)
		assert.NoError(t, err, "should not error when trying to remove a nil batch")
	})

	t.Run("Check empty key", func(t *testing.T) {
		store, ctx := setupMockEntityStore(t, rsClient)
		exists, err := store.Exists(ctx, "")
		assert.False(t, exists)
		assert.NoError(t, err, "should not error when checking if an entity exists with an empty key")
	})
}
