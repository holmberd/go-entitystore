package keyfactory

import (
	"fmt"
	"testing"

	"github.com/holmberd/go-entitystore/keyfactory/internal/rediskey"
)

func TestRedisKeyBuilder(t *testing.T) {
	tests := []struct {
		name         string
		keyNamespace string
		wildcard     rediskey.GlobWildcard
		parentKey    string
		key          string
		expectKey    string
		expectError  bool
	}{
		{
			name:      "Key without namespaces",
			key:       "entity:entity1",
			expectKey: "entity:entity1",
		},
		{
			name:         "Key with namespace",
			keyNamespace: "group1",
			key:          "tenant1:entity:entity1",
			expectKey:    "__group1__:tenant1:entity:entity1",
		},
		{
			name:      "Key with parent key",
			parentKey: "tenant:tenant1",
			key:       "entity:entity1",
			expectKey: "tenant:tenant1:entity:entity1",
		},
		{
			name:         "Key with namespace and parent key",
			keyNamespace: "group1",
			parentKey:    "tenant:tenant1",
			key:          "entity:entity1",
			expectKey:    "__group1__:tenant:tenant1:entity:entity1",
		},
		{
			name:      "Key with any string wildcard",
			parentKey: "tenant:tenant1",
			key:       "entity",
			wildcard:  WildcardAnyString,
			expectKey: fmt.Sprintf("tenant:tenant1:entity:%s", WildcardAnyString),
		},
		{
			name:      "Key with any char wildcard",
			parentKey: "tenant:tenant1",
			key:       "entity",
			wildcard:  WildcardAnyChar,
			expectKey: fmt.Sprintf("tenant:tenant1:entity:%s", WildcardAnyChar),
		},
		{
			name:         "Key with invalid namespace",
			keyNamespace: "__group",
			key:          "tenant1:entity:entity1",
			expectError:  true,
		},
		{
			name:        "Invalid key contains reserved namespace delimiter",
			key:         "__entity:entity1",
			expectError: true,
		},
		{
			name:        "Invalid key is invalid redis key",
			key:         ":entity1",
			expectError: true,
		},
		{
			name:        "Invalid parent key",
			parentKey:   "__tenant:tenant1",
			key:         "entity:entity1",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewKeyBuilder()
			builder.WithNamespace(tt.keyNamespace)
			builder.WithParentKey(tt.parentKey)
			builder.WithKey(tt.key)
			builder.WithWildcard(tt.wildcard)
			key, err := builder.Build()

			if tt.expectError && err == nil {
				t.Errorf("expected an error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if key != nil && key.RedisKey() != tt.expectKey {
				t.Errorf("expected key: %q, got: %q", tt.expectKey, key)
			}
		})
	}
}
