package keyfactory

import (
	"fmt"

	"github.com/holmberd/go-entitystore/keyfactory/internal/rediskey"
)

type EntityKind string

const (
	EntityKindTenant EntityKind = "tenant"
	EntityKindTest   EntityKind = "test_entity"
)

func validEntityKinds() []EntityKind {
	return []EntityKind{
		EntityKindTenant,
		EntityKindTest,
	}
}

func validateEntityKind(k EntityKind) error {
	for _, valid := range validEntityKinds() {
		if k == valid {
			return nil
		}
	}
	return fmt.Errorf("keyfactory: invalid entity kind: %q", k)
}

// NewTenantKey returns a new structured logical tenant key.
//
// Key structure:
//
//	<tenantKind>:<tenantId>
func NewTenantKey(id string) (string, error) {
	if err := validateKeyFragments(id); err != nil {
		return "", fmt.Errorf("keyfactory: %w", err)
	}
	key, err := rediskey.New(string(EntityKindTenant), id)
	if err != nil {
		return "", fmt.Errorf("keyfactory: %w", err)
	}
	return key, nil
}

// NewEntityKey returns a new structured logical entity key.
//
// Key structure:
//
//	<parentEntityKey>:<entityKind>:<entityId>:<entityVersionId>
func NewEntityKey(
	entityKind EntityKind,
	entityId string,
	entityVersionId string, // Optional entity state version ID.
	parentEntityKey string, // Optional parent entity key.
) (string, error) {

	if err := validateEntityKind(entityKind); err != nil {
		return "", err
	}
	if entityKind == "" {
		return "", fmt.Errorf("keyfactory: entity kind must not be empty")
	}
	if entityId == "" {
		return "", fmt.Errorf("keyfactory: entity ID must not be empty")
	}
	if err := validateKeyFragments(string(entityKind), entityId, entityVersionId, parentEntityKey); err != nil {
		return "", fmt.Errorf("keyfactory: %w", err)
	}
	key, err := rediskey.New(string(entityKind), entityId, entityVersionId)
	if err != nil {
		return "", fmt.Errorf("keyfactory: %w", err)
	}
	if parentEntityKey != "" {
		if err := rediskey.Validate(parentEntityKey); err != nil {
			return "", fmt.Errorf("keyfactory: %w", err)
		}
		return rediskey.Build(parentEntityKey, key), nil
	}
	return key, nil
}
