package entitystore

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/holmberd/go-entitystore/datastore"
	"github.com/holmberd/go-entitystore/entitystore/pb"
	"github.com/holmberd/go-entitystore/keyfactory"
	"github.com/holmberd/go-entitystore/testutil"
	"google.golang.org/protobuf/proto"
)

type TestEntity struct {
	Key       string
	Id        string
	TenantId  string
	UpdatedAt int64
}

func NewTestEntity(
	id string,
	tenantId string,
) (*TestEntity, error) {
	e := &TestEntity{
		Id:        id,
		TenantId:  tenantId,
		UpdatedAt: time.Now().Unix(),
	}
	parentKey, err := keyfactory.NewTenantKey(tenantId)
	if err != nil {
		return nil, err
	}
	e.Key, err = keyfactory.NewEntityKey(
		keyfactory.EntityKindTest,
		e.Id,
		strconv.FormatInt(e.UpdatedAt, 10),
		parentKey,
	)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e TestEntity) GetKey() string {
	return e.Key
}

func (e TestEntity) ToProto() (*pb.TestEntity, error) {
	return &pb.TestEntity{
		Id:        e.Id,
		TenantId:  e.TenantId,
		UpdatedAt: e.UpdatedAt,
	}, nil
}

func (e *TestEntity) FromProto(pbEntity *pb.TestEntity) error {
	parentKey, err := keyfactory.NewTenantKey(pbEntity.GetTenantId())
	if err != nil {
		return err
	}
	key, err := keyfactory.NewEntityKey(
		keyfactory.EntityKindTest,
		pbEntity.GetId(),
		strconv.FormatInt(pbEntity.GetUpdatedAt(), 10),
		parentKey,
	)
	if err != nil {
		return err
	}
	*e = TestEntity{
		Key:       key,
		Id:        pbEntity.GetId(),
		TenantId:  pbEntity.GetTenantId(),
		UpdatedAt: pbEntity.GetUpdatedAt(),
	}
	return nil
}

// MarshalProto marshals an entity into protobuf bytes (implements ProtoMarshaler).
func (e TestEntity) MarshalProto() ([]byte, error) {
	pbe, err := e.ToProto()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(pbe)
}

// UnmarshalProto unmarshals protobuf bytes into an entity (implements ProtoUnmarshaler).
func (e *TestEntity) UnmarshalProto(data []byte) error {
	pbe := &pb.TestEntity{}
	if err := proto.Unmarshal(data, pbe); err != nil {
		return fmt.Errorf("failed to unmarshal entity: %w", err)
	}
	return e.FromProto(pbe)
}

type TEntityStore struct {
	*EntityStore[TestEntity, *TestEntity]
}

func NewTEntityStore(namespace string, dsClient *datastore.Client) (*TEntityStore, error) {
	entityStore, err := New[TestEntity](
		string(keyfactory.EntityKindTest),
		namespace,
		dsClient,
	)
	if err != nil {
		return nil, err
	}
	return &TEntityStore{
		EntityStore: entityStore,
	}, nil
}

func setupTEntityStore(
	t *testing.T,
	ctx context.Context,
	entityKind string,
	namespace string,
	dsClient *datastore.Client,
) EntityStorer[TestEntity, *TestEntity] {
	t.Helper()
	store, err := NewTEntityStore(namespace, dsClient)
	if err != nil {
		t.Fatalf("failed to setup entity store: %v", err)
	}
	return store
}

func generateTestEntities(t *testing.T, num int, tenantId string) ([]TestEntity, []string) {
	t.Helper()
	var entities []TestEntity
	keys := make([]string, 0, num)
	for i := 1; i <= num; i++ {
		id := fmt.Sprintf("%s%d", "e-", i)
		e, _ := NewTestEntity(id, tenantId)
		entities = append(entities, *e)
		keys = append(keys, e.GetKey())
	}
	return entities, keys
}

func TestTEntityStore(t *testing.T) {
	rsClient, server := testutil.NewRedisClientWithCleanup(t)
	defer server.Close()

	dsClient, err := datastore.NewClient(rsClient)
	if err != nil {
		t.Fatalf("failed to create datastore client: %v", err)
	}

	suite := NewEntityStoreTestSuite(
		t,
		string(keyfactory.EntityKindTest),
		dsClient,
		setupTEntityStore,
		generateTestEntities,
	)
	suite.Run(t)
}
