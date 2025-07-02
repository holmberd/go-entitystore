package entitystore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/holmberd/go-entitystore/datastore"
	"github.com/holmberd/go-entitystore/encoder"
	"github.com/holmberd/go-entitystore/eventemitter"
	"github.com/holmberd/go-entitystore/keyfactory"
)

const Nil = EntityStoreError("entitystore: nil")

type EntityStoreError string

func (e EntityStoreError) Error() string { return string(e) }

type EntityStorer[T Entity, PT SerializableEntity[T]] interface {
	flush(ctx context.Context) error
	Add(ctx context.Context, entity T, expiration time.Duration) (string, error)
	AddBatch(ctx context.Context, entities []T, expiration time.Duration) ([]string, error)
	Remove(ctx context.Context, entityKey string) error
	RemoveByKeys(ctx context.Context, entityKeys []string) error
	RemoveAll(ctx context.Context, parentKey string) error
	Get(ctx context.Context, entityKey string) (PT, error)
	GetByKeys(ctx context.Context, entityKeys []string) ([]PT, error)
	GetWithPagination(ctx context.Context, cursor uint64, limit int, parentKey string) (*EntityCursor[T, PT], error)
	GetAll(ctx context.Context, parentKey string) ([]PT, error)
	Exists(ctx context.Context, entityKey string) (bool, error)
	OnAdded() *eventTarget
	OnUpdated() *eventTarget
	OnRemoved() *eventTarget
}

type Event int

const (
	EntitiesAdded Event = iota
	EntitiesRemoved
	EntitiesUpdated
	EntitiesFlushed
)

func (e Event) String() string {
	switch e {
	case EntitiesAdded:
		return "EntitiesAdded"
	case EntitiesRemoved:
		return "EntitiesRemoved"
	case EntitiesUpdated:
		return "EntitiesUpdated"
	case EntitiesFlushed:
		return "EntitiesFlushed"
	default:
		return fmt.Sprintf("event(%d)", e)
	}
}

type Entity interface {
	GetKey() string // Entity structured unique datastore key.
}

// SerializableEntity represents an entity that can be serialized/deserialized.
type SerializableEntity[T Entity] interface {
	*T // Ensures T is a value type and *T is a pointer.
	Entity
	encoder.ProtoMarshaler
	encoder.ProtoUnmarshaler
}

// EntityCursor is a cursors for paginated entity retrieval from a store.
type EntityCursor[T Entity, PT SerializableEntity[T]] struct {
	Cursor   uint64
	Entities []PT
}

type EntityStoreListener func(ctx context.Context, keys []string)

type eventTarget struct {
	t *eventemitter.EventTarget
}

func (e *eventTarget) AddListener(listener EntityStoreListener) eventemitter.ListenerToken {
	return e.t.AddListener(func(args ...any) {
		if len(args) < 2 {
			log.Panicf("missing arguments in %s event listener", EntitiesAdded)
		}
		ctx, ok := args[0].(context.Context)
		if !ok {
			log.Panicf("argument is not of expected type %T (got %T)", context.Background(), args[0])
		}
		keys, ok := args[1].([]string)
		if !ok {
			log.Panicf("argument is not of expected type %T (got %T)", []string{}, args[1])
		}
		listener(ctx, keys)
	})
}

func (e *eventTarget) RemoveListener(token eventemitter.ListenerToken) bool {
	return e.t.RemoveListener(token)
}

func (e *eventTarget) emit(ctx context.Context, keys []string) bool {
	return e.t.Emit(ctx, keys)
}

// EntityStore provides a reusable datastore implementation for an entity kind/type.
type EntityStore[T Entity, PT SerializableEntity[T]] struct {
	entityKind string // Required logical entity identifier.
	namespace  string // Optional key namespace.
	dsClient   *datastore.Client
	onAdded    *eventTarget
	onRemoved  *eventTarget
	onUpdated  *eventTarget
	onFlushed  *eventTarget
}

// NewEntityStore creates a new instance of a store.
func New[T Entity, PT SerializableEntity[T]](
	entityKind string,
	namespace string,
	dsClient *datastore.Client,
) (*EntityStore[T, PT], error) {
	if entityKind == "" {
		return nil, errors.New("entity kind must not be empty")
	}
	if namespace != "" {
		if err := keyfactory.ValidateKeyFragment(namespace); err != nil {
			return nil, err
		}
	}
	return &EntityStore[T, PT]{
		entityKind: entityKind,
		namespace:  namespace,
		dsClient:   dsClient,
		onAdded:    &eventTarget{eventemitter.NewEventTarget(EntitiesAdded.String())},
		onRemoved:  &eventTarget{eventemitter.NewEventTarget(EntitiesRemoved.String())},
		onUpdated:  &eventTarget{eventemitter.NewEventTarget(EntitiesUpdated.String())},
		onFlushed:  &eventTarget{eventemitter.NewEventTarget(EntitiesFlushed.String())},
	}, nil
}

func (es *EntityStore[T, PT]) EntityKind() string {
	return es.entityKind
}

func (es *EntityStore[T, PT]) NewKeyBuilder() *keyfactory.KeyBuilderWithNamespace {
	return keyfactory.NewKeyBuilderWithNamespace(es.namespace)
}

func (es *EntityStore[T, PT]) OnAdded() *eventTarget {
	return es.onAdded
}

func (es *EntityStore[T, PT]) OnUpdated() *eventTarget {
	return es.onUpdated
}

func (es *EntityStore[T, PT]) OnRemoved() *eventTarget {
	return es.onRemoved
}

func (es *EntityStore[T, PT]) OnFlushed() *eventTarget {
	return es.onFlushed
}

// flush deletes all keys in the key namespace, used in e.g. tests.
// It triggers the EntitiesFlushed event.
func (es *EntityStore[T, PT]) flush(ctx context.Context) error {
	if es.namespace == "" {
		log.Panic("flush store called without key namespace set")
	}
	kb := es.NewKeyBuilder()
	kb.WithWildcard(keyfactory.WildcardAnyString)
	keyMatch, err := kb.BuildAndReset()
	if err != nil {
		return err
	}
	err = es.dsClient.DeleteMatch(ctx, keyMatch)
	if err != nil {
		return err
	}
	es.onFlushed.emit(ctx, []string{})
	return nil
}

// Add adds an entity to the store.
// If the entity doesn't exist it's added, otherwise it's updated.
func (es *EntityStore[T, PT]) Add(ctx context.Context, entity T, expiration time.Duration) (string, error) {
	kb := es.NewKeyBuilder()
	kb.WithKey(entity.GetKey())
	key, err := kb.BuildAndReset()
	if err != nil {
		return "", err
	}
	data, err := encoder.ProtoMarshal(PT(&entity))
	if err != nil {
		return "", err
	}
	if err = es.dsClient.Put(ctx, key, data, expiration); err != nil {
		return "", err
	}
	es.onAdded.emit(ctx, []string{entity.GetKey()})
	return entity.GetKey(), nil
}

// AddBatch adds multiple entities in a batch operation to the store.
func (es *EntityStore[T, PT]) AddBatch(
	ctx context.Context,
	entities []T,
	expiration time.Duration,
) ([]string, error) {
	if len(entities) == 0 {
		return nil, nil // No-op for empty batch.
	}

	kb := es.NewKeyBuilder()
	keys := make([]*keyfactory.Key, len(entities))
	entityKeys := make([]string, len(keys))
	data := make([][]byte, len(keys))
	for i, entity := range entities {
		kb.WithKey(entity.GetKey())
		key, err := kb.BuildAndReset()
		if err != nil {
			return nil, err
		}
		d, err := encoder.ProtoMarshal(PT(&entity))
		if err != nil {
			return nil, fmt.Errorf("failed to marshal entity with key '%s': %w", entity.GetKey(), err)
		}
		data[i] = d
		entityKeys[i] = entity.GetKey()
		keys[i] = key
	}
	if err := es.dsClient.PutMulti(ctx, keys, data, expiration); err != nil {
		return nil, err
	}
	es.onAdded.emit(ctx, entityKeys)
	return entityKeys, nil
}

// Remove removes an entity by key from the store.
func (es *EntityStore[T, PT]) Remove(ctx context.Context, entityKey string) error {
	if entityKey == "" {
		return nil // No-op for empty key.
	}
	kb := es.NewKeyBuilder()
	kb.WithKey(entityKey)
	key, err := kb.BuildAndReset()
	if err != nil {
		return err
	}
	if err = es.dsClient.Delete(ctx, key); err != nil {
		return err
	}
	es.onRemoved.emit(ctx, []string{entityKey})
	return nil
}

// RemoveByKeys removes multiple entities by their keys from the store.
func (es *EntityStore[T, PT]) RemoveByKeys(ctx context.Context, entityKeys []string) error {
	if len(entityKeys) == 0 {
		return nil // No-op for empty key.
	}
	keys := make([]*keyfactory.Key, len(entityKeys))
	kb := es.NewKeyBuilder()
	for i, eKey := range entityKeys {
		kb.WithKey(eKey)
		key, err := kb.BuildAndReset()
		if err != nil {
			return err
		}
		keys[i] = key
	}
	if err := es.dsClient.Delete(ctx, keys...); err != nil {
		return err
	}
	es.onRemoved.emit(ctx, entityKeys)
	return nil
}

// RemoveAll removes all entities from the store.
//
// NOTE: This is a blocking operation.
func (es *EntityStore[T, PT]) RemoveAll(ctx context.Context, parentKey string) error {
	kb := es.NewKeyBuilder()
	kb.WithParentKey(parentKey)
	kb.WithKey(es.entityKind)
	kb.WithWildcard(keyfactory.WildcardAnyString)
	keyMatch, err := kb.BuildAndReset()
	if err != nil {
		return err
	}
	keys, err := es.dsClient.GetKeys(ctx, keyMatch)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil // No-op.
	}
	if err := es.dsClient.Delete(ctx, keys...); err != nil {
		return err
	}

	entityKeys := make([]string, len(keys))
	for i, key := range keys {
		entityKeys[i] = key.Key()
	}
	es.onRemoved.emit(ctx, entityKeys)
	return nil
}

// Get retrieves an entity by key from the store.
// datastore.ErrKeyNotFound is returned if key is not found in the store.
func (es *EntityStore[T, PT]) Get(ctx context.Context, entityKey string) (PT, error) {
	if entityKey == "" {
		return nil, nil // No-op for empty key.
	}
	kb := es.NewKeyBuilder()
	kb.WithKey(entityKey)
	key, err := kb.BuildAndReset()
	if err != nil {
		return nil, err
	}
	data, err := es.dsClient.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	entityPtr := PT(new(T))
	err = encoder.ProtoUnmarshal(data, entityPtr)
	if err != nil {
		return nil, err
	}
	return entityPtr, nil
}

// GetByKeys retrieves multiple entities by their keys from the store.
// If a key doesn't exist in the store it is not included in the result.
func (es *EntityStore[T, PT]) GetByKeys(ctx context.Context, entityKeys []string) ([]PT, error) {
	if len(entityKeys) == 0 {
		return nil, nil // No-op for empty slice of keys.
	}
	kb := es.NewKeyBuilder()
	keys := make([]*keyfactory.Key, len(entityKeys))
	for i, eKey := range entityKeys {
		if eKey == "" {
			continue // Skip empty keys.
		}
		kb.WithKey(eKey)
		key, err := kb.BuildAndReset()
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}

	data, err := es.dsClient.GetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}
	entities := make([]PT, len(data))
	for i, d := range data {
		entities[i] = PT(new(T))
		err = encoder.ProtoUnmarshal(d, entities[i])
		if err != nil {
			return nil, err
		}
	}
	return entities, nil
}

// GetWithPagination retrieves entities from the store with cursor pagination.
//   - Does not gurantee an exact number of entities returned per page.
//   - A given entity may be returned multiple times.
//   - Entities that were not constantly present in the collection during a full iteration, may be returned or not.
func (es *EntityStore[T, PT]) GetWithPagination(
	ctx context.Context,
	cursor uint64,
	limit int,
	parentKey string,
) (*EntityCursor[T, PT], error) {
	if limit <= 0 || limit >= 1000 {
		limit = 1000 // Enforce max-limit.
	}
	kb := es.NewKeyBuilder()
	kb.WithParentKey(parentKey)
	kb.WithKey(es.entityKind)
	kb.WithWildcard(keyfactory.WildcardAnyString)
	keyMatch, err := kb.BuildAndReset()
	if err != nil {
		return nil, err
	}

	// Get page keys.
	keys, nextCursor, err := es.dsClient.GetKeysWithCursor(ctx, cursor, limit, keyMatch)
	if err != nil {
		return nil, err
	}
	cursor = nextCursor

	if len(keys) == 0 {
		return &EntityCursor[T, PT]{Cursor: cursor, Entities: nil}, nil
	}

	// Get page entities.
	data, err := es.dsClient.GetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}
	entities := make([]PT, len(data))
	for i, d := range data {
		entities[i] = PT(new(T))
		err = encoder.ProtoUnmarshal(d, entities[i])
		if err != nil {
			return nil, err
		}
	}
	return &EntityCursor[T, PT]{
		Cursor:   cursor,
		Entities: entities,
	}, nil
}

// GetAll retrieves all entities from the store.
// If a key doesn't exist in the store it is not included in the result.
//
// NOTE: This is a blocking operation.
//
// TODO: Consider adding alternative implementation using SCAN if needed.
func (es *EntityStore[T, PT]) GetAll(ctx context.Context, parentKey string) ([]PT, error) {
	kb := es.NewKeyBuilder()
	kb.WithParentKey(parentKey)
	kb.WithKey(es.entityKind)
	kb.WithWildcard(keyfactory.WildcardAnyString)
	keyMatch, err := kb.BuildAndReset()
	if err != nil {
		return nil, err
	}
	keys, err := es.dsClient.GetKeys(ctx, keyMatch)
	if err != nil {
		return nil, err
	}
	data, err := es.dsClient.GetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}
	entities := make([]PT, len(data))
	for i, d := range data {
		entities[i] = PT(new(T))
		err = encoder.ProtoUnmarshal(d, entities[i])
		if err != nil {
			return nil, err
		}
	}
	return entities, nil
}

// Exists checks whether an entity exist in the store.
func (es *EntityStore[T, PT]) Exists(ctx context.Context, entityKey string) (bool, error) {
	if entityKey == "" {
		return false, nil // No-op for empty key.
	}
	kb := es.NewKeyBuilder()
	kb.WithKey(entityKey)
	key, err := kb.BuildAndReset()
	if err != nil {
		return false, err
	}
	exists, err := es.dsClient.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	return exists, nil
}
