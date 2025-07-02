package entitystore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holmberd/go-entitystore/datastore"
	"github.com/holmberd/go-entitystore/keyfactory"
	"github.com/holmberd/go-entitystore/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const mockTenantId = "mock_tenant1"

var mockTenantKey, _ = keyfactory.NewTenantKey(mockTenantId)

// EntityStoreTestSuite provides a full test suite for any entity implementing the EntityStore.
type EntityStoreTestSuite[T Entity, PT SerializableEntity[T]] struct {
	EntityKind string
	DSClient   *datastore.Client

	// SetupStore initializes a new store with test data isolation and cleanup.
	SetupStore       func(t *testing.T) (EntityStorer[T, PT], context.Context)
	GenerateEntities func(t *testing.T, num int, tenantId string) ([]T, []string)
}

func NewEntityStoreTestSuite[T Entity, PT SerializableEntity[T]](
	t *testing.T,
	entityKind string,
	dsClient *datastore.Client,
	setupStore func(
		t *testing.T,
		ctx context.Context,
		entityKind string,
		namespace string,
		dsClient *datastore.Client,
	) EntityStorer[T, PT],
	generateEntities func(t *testing.T, num int, tenantId string) ([]T, []string),
) *EntityStoreTestSuite[T, PT] {
	return &EntityStoreTestSuite[T, PT]{
		EntityKind: entityKind,
		DSClient:   dsClient,
		SetupStore: func(t *testing.T) (EntityStorer[T, PT], context.Context) {
			ctx := context.Background() // New context for each store to ensure test isolation.

			// Set a unique random key as namespace to isolate any keys written by the
			// store during testing. This ensures test data isolation in concurrent tests.
			namespace := keyfactory.GenerateRandomKey() // Random key namespace to ensure test data isolation.
			store := setupStore(t, ctx, entityKind, namespace, dsClient)

			t.Cleanup(func() {
				// Flush the store data after each test.
				// TODO: Not necessary when using testutil.NewRedisClientWithCleanup.
				err := store.flush(ctx)
				if err != nil {
					t.Fatalf("failed to flush store data after test: %v", err)
				}
			})
			return store, ctx
		},
		GenerateEntities: func(t *testing.T, num int, tenantId string) ([]T, []string) {
			return generateEntities(t, num, tenantId)
		},
	}
}

func (s *EntityStoreTestSuite[T, PT]) Run(t *testing.T) {
	t.Run(fmt.Sprintf("Test %s GenerateEntites", s.EntityKind), s.TestGenerateEntities)
	t.Run(fmt.Sprintf("Test %s Add", s.EntityKind), s.TestAdd)
	t.Run(fmt.Sprintf("Test %s AddBatch", s.EntityKind), s.TestAddBatch)
	t.Run(fmt.Sprintf("Test %s Get", s.EntityKind), s.TestGet)
	t.Run(fmt.Sprintf("Test %s GetByKeys", s.EntityKind), s.TestGetByKeys)
	t.Run(fmt.Sprintf("Test %s GetWithPagination", s.EntityKind), s.TestGetWithPagination)
	t.Run(fmt.Sprintf("Test %s GetAll", s.EntityKind), s.TestGetAll)
	t.Run(fmt.Sprintf("Test %s Exists", s.EntityKind), s.TestExists)
	t.Run(fmt.Sprintf("Test %s RemoveAll", s.EntityKind), s.TestRemoveAll)
	t.Run(fmt.Sprintf("Test %s Remove", s.EntityKind), s.TestRemove)
	t.Run(fmt.Sprintf("Test %s RemoveByKeys", s.EntityKind), s.TestRemoveByKeys)
}

func (s *EntityStoreTestSuite[T, PT]) TestGenerateEntities(t *testing.T) {
	numEntities := 10
	entities, keys := s.GenerateEntities(t, numEntities, mockTenantId)
	assert.Len(t, entities, numEntities, "should generate the correct number of entities")
	assert.Len(t, keys, numEntities, "should generate the correct number of entity keys")
}

func (s *EntityStoreTestSuite[T, PT]) TestAdd(t *testing.T) {
	t.Run("Add entity", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, _ := s.GenerateEntities(t, 1, mockTenantId)
		_, err := store.Add(ctx, entities[0], 0)
		assert.NoError(t, err, "should not error when adding an entity")
	})

	t.Run("Add entity triggers synchronous event listeners", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 1, mockTenantId)
		var receivedCtx context.Context
		var receivedKeys []string
		listenerToken := store.OnAdded().AddListener(func(ctx context.Context, keys []string) {
			receivedCtx = ctx
			receivedKeys = keys
		})
		defer store.OnAdded().RemoveListener(listenerToken)

		_, err := store.Add(ctx, entities[0], 0)
		assert.NoError(t, err)
		assert.ElementsMatch(t, keys, receivedKeys, "should match entity keys")
		assert.Equal(t, ctx, receivedCtx, "should match the received context")
	})

	t.Run("Add an entity triggers asynchronous event listeners", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 1, mockTenantId)
		var wg sync.WaitGroup
		var received atomic.Value

		listenerToken := store.OnAdded().AddListener(func(ctx context.Context, keys []string) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				received.Store(struct {
					ctx  context.Context
					keys []string
				}{ctx, keys})
			}()
		})
		defer store.OnAdded().RemoveListener(listenerToken)

		_, err := store.Add(ctx, entities[0], 0)
		assert.NoError(t, err, "should not error when adding an entity")

		testutil.WaitGroupWithTimeout(t, &wg, time.Second)
		v := received.Load()
		assert.NotNil(t, v)
		r, ok := v.(struct {
			ctx  context.Context
			keys []string
		})
		assert.True(t, ok)
		assert.ElementsMatch(t, keys, r.keys, "should match entity keys")
		assert.Equal(t, ctx, r.ctx, "should match the received context")
	})
}

func (s *EntityStoreTestSuite[T, PT]) TestAddBatch(t *testing.T) {
	t.Run("Add entities", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 10, mockTenantId)
		_, err := store.AddBatch(ctx, entities, 0)
		assert.NoError(t, err, "should not error when adding entities in batch")
		retrieved, err := store.GetByKeys(ctx, keys)
		assert.NoError(t, err)
		assert.Len(t, retrieved, len(keys))
	})

	t.Run("Add entities triggers event listeners", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 10, mockTenantId)
		var receivedCtx context.Context
		var receivedKeys []string
		listenerToken := store.OnAdded().AddListener(func(ctx context.Context, keys []string) {
			receivedCtx = ctx
			receivedKeys = keys
		})
		defer store.OnAdded().RemoveListener(listenerToken)

		_, err := store.AddBatch(ctx, entities, 0)
		assert.NoError(t, err)
		assert.ElementsMatch(t, keys, receivedKeys, "should match entity keys")
		assert.Equal(t, ctx, receivedCtx, "should match the received context")
	})
}

func (s *EntityStoreTestSuite[T, PT]) TestGet(t *testing.T) {
	t.Run("Retrieve existing entity", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 1, mockTenantId)
		_, err := store.Add(ctx, entities[0], 0)
		assert.NoError(t, err)
		entityOut, err := store.Get(ctx, keys[0])
		assert.NoError(t, err, "should not error when retrieving an existing entity")
		assert.Equal(t, entities[0], *entityOut, "retrieved entity should match stored data")
	})
}

func (s *EntityStoreTestSuite[T, PT]) TestGetByKeys(t *testing.T) {
	t.Run("Retrieve multiple existing entities", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 3, mockTenantId)
		assert.Len(t, entities, 3)
		_, err := store.AddBatch(ctx, entities, 0)
		assert.NoError(t, err)
		retrieved, err := store.GetByKeys(ctx, keys)
		assert.NoError(t, err)
		assert.Len(t, retrieved, len(keys))
		retrievedValues := make([]T, len(retrieved))
		for i := range retrieved {
			retrievedValues[i] = *retrieved[i]
		}
		assert.ElementsMatch(t, retrievedValues, entities)
	})
}

func (s *EntityStoreTestSuite[T, PT]) TestGetWithPagination(t *testing.T) {
	t.Run("Fetch paginated entities", func(t *testing.T) {
		numEntities := 25
		store, ctx := s.SetupStore(t)
		entities, _ := s.GenerateEntities(t, numEntities, mockTenantId)
		assert.Len(t, entities, numEntities, fmt.Sprintf("should generate %d entities", numEntities))
		_, err := store.AddBatch(ctx, entities, 0)
		assert.NoError(t, err)

		addedEntities, err := store.GetAll(ctx, mockTenantKey)
		assert.NoError(t, err)
		assert.Len(t, addedEntities, numEntities, fmt.Sprintf("should have added %d entities", numEntities))

		cursor := uint64(0)
		limit := 10
		retrievedEntities := make(map[string]bool)

		for {
			resp, err := store.GetWithPagination(ctx, cursor, limit, mockTenantKey)
			assert.NoError(t, err, "should not error when fetching paginated entities")

			// Filter out potential duplicate entities.
			for _, entity := range resp.Entities {
				key := entity.GetKey()
				assert.NoError(t, err, "should not error when generating entity key")
				retrievedEntities[key] = true
			}

			if resp.Cursor == 0 {
				break
			}
			cursor = resp.Cursor
		}
		assert.Len(t, retrievedEntities, numEntities, fmt.Sprintf("should retrive all %d entities", numEntities))
	})
}

func (s *EntityStoreTestSuite[T, PT]) TestGetAll(t *testing.T) {
	t.Run("Retrieve all entities", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, _ := s.GenerateEntities(t, 25, mockTenantId)
		_, err := store.AddBatch(ctx, entities, 0)
		assert.NoError(t, err)

		allEntities, _ := store.GetAll(ctx, mockTenantKey)
		assert.NoError(t, err)
		assert.Len(t, allEntities, len(entities))
	})
}

func (s *EntityStoreTestSuite[T, PT]) TestExists(t *testing.T) {
	t.Run("Check existence of non-existent entity", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		_, keys := s.GenerateEntities(t, 1, mockTenantId)
		exists, err := store.Exists(ctx, keys[0])
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("Check existence after adding entity", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 1, mockTenantId)
		_, err := store.Add(ctx, entities[0], 0)
		assert.NoError(t, err)

		exists, err := store.Exists(ctx, keys[0])
		assert.NoError(t, err)
		assert.True(t, exists)
	})
}

func (s *EntityStoreTestSuite[T, PT]) TestRemoveAll(t *testing.T) {
	t.Run("Remove all entities", func(t *testing.T) {
		numEntities := 12
		store, ctx := s.SetupStore(t)
		entities, _ := s.GenerateEntities(t, numEntities, mockTenantId)
		_, err := store.AddBatch(ctx, entities, 0)
		assert.NoError(t, err)

		entitiesIn, err := store.GetAll(ctx, mockTenantKey)
		assert.NoError(t, err)
		assert.Len(t, entitiesIn, numEntities)

		err = store.RemoveAll(ctx, mockTenantKey)
		assert.NoError(t, err)

		entitiesOut, err := store.GetAll(ctx, mockTenantKey)
		assert.NoError(t, err)
		assert.Len(t, entitiesOut, 0)
	})
}

func (s *EntityStoreTestSuite[T, PT]) TestRemove(t *testing.T) {
	t.Run("Remove existing entity", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 1, mockTenantId)
		_, err := store.Add(ctx, entities[0], 0)
		assert.NoError(t, err)

		exists, err := store.Exists(ctx, keys[0])
		assert.NoError(t, err)
		assert.True(t, exists)

		err = store.Remove(ctx, keys[0])
		assert.NoError(t, err)

		exists, err = store.Exists(ctx, keys[0])
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("Remove entity triggers event listeners", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 1, mockTenantId)
		_, err := store.Add(ctx, entities[0], 0)
		assert.NoError(t, err)

		var receivedCtx context.Context
		var receivedKeys []string
		listenerToken := store.OnRemoved().AddListener(func(ctx context.Context, keys []string) {
			receivedCtx = ctx
			receivedKeys = keys
		})
		defer store.OnRemoved().RemoveListener(listenerToken)

		err = store.Remove(ctx, keys[0])
		assert.NoError(t, err)
		assert.ElementsMatch(t, keys, receivedKeys, "should match entity keys")
		assert.Equal(t, ctx, receivedCtx, "should match the received context")
	})
}

func (s *EntityStoreTestSuite[T, PT]) TestRemoveByKeys(t *testing.T) {
	t.Run("Remove multiple entities", func(t *testing.T) {
		numEntities := 3
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, numEntities, mockTenantId)
		_, err := store.AddBatch(ctx, entities, 0)
		assert.NoError(t, err)

		entitiesIn, err := store.GetByKeys(ctx, keys)
		assert.NoError(t, err)
		assert.Len(t, entitiesIn, numEntities)

		err = store.RemoveByKeys(ctx, keys)
		assert.NoError(t, err)
		for _, key := range keys {
			exists, err := store.Exists(ctx, key)
			assert.NoError(t, err)
			assert.False(t, exists)
		}
	})

	t.Run("Remove a mix of existing and non-existent entities", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 2, mockTenantId)
		_, err := store.AddBatch(ctx, entities, 0)
		require.NoError(t, err, "should not error when adding enties")

		// Assert added entities exists.
		for _, key := range keys {
			exists, err := store.Exists(ctx, key)
			assert.NoError(t, err, "should not error when checking existence")
			assert.True(t, exists, "entity should exist after being added")
		}

		// Try removing the re-added pool plus some non-existent ones.
		nonExistentKeys := []string{"non-existent-1", "non-existent-2"}
		removeKeys := append(nonExistentKeys, keys...)

		err = store.RemoveByKeys(ctx, removeKeys)
		assert.NoError(t, err, "should not error when removing mixed existing and non-existent entities")

		// Assert added entities where removed.
		for _, key := range keys {
			exists, err := store.Exists(ctx, key)
			assert.NoError(t, err, "should not error when checking existence")
			assert.False(t, exists, "entity shouldn't exist after being removed")
		}
	})

	t.Run("Remove entities triggers event listeners", func(t *testing.T) {
		store, ctx := s.SetupStore(t)
		entities, keys := s.GenerateEntities(t, 3, mockTenantId)
		_, err := store.AddBatch(ctx, entities, 0)
		assert.NoError(t, err)

		var receivedCtx context.Context
		var receivedKeys []string
		listenerToken := store.OnRemoved().AddListener(func(ctx context.Context, keys []string) {
			receivedCtx = ctx
			receivedKeys = keys
		})
		defer store.OnRemoved().RemoveListener(listenerToken)

		err = store.RemoveByKeys(ctx, keys)
		assert.NoError(t, err)
		assert.ElementsMatch(t, keys, receivedKeys, "should match entity keys")
		assert.Equal(t, ctx, receivedCtx, "should match the received context")
	})
}
