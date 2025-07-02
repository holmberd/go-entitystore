package keyfactory

// TODO: Refactor tests.

// func TestNewEntityKey(t *testing.T) {
// 	type keyInput struct {
// 		entityName      string
// 		entityId        string
// 		entityVersionId string
// 		parentEntityKey string
// 	}

// 	tests := []struct {
// 		name        string
// 		input       keyInput
// 		expectKey   string
// 		expectError bool
// 	}{
// 		{
// 			name: "Entity key without version ID or parent key",
// 			input: keyInput{
// 				entityName: "entity1",
// 				entityId:   "123",
// 			},
// 			expectKey: "entity1:123",
// 		},
// 		{
// 			name: "Entity key with version ID",
// 			input: keyInput{
// 				entityName:      "entity1",
// 				entityId:        "123",
// 				entityVersionId: "1",
// 			},
// 			expectKey: "entity1:123:1",
// 		},
// 		{
// 			name: "Entity key with parent entity key",
// 			input: keyInput{
// 				entityName:      "entity1",
// 				entityId:        "123",
// 				parentEntityKey: "tenant:tenant1",
// 			},
// 			expectKey: "tenant:tenant1:entity1:123",
// 		},
// 		{
// 			name: "Entity key with version ID and parent entity key",
// 			input: keyInput{
// 				entityName:      "entity1",
// 				entityId:        "123",
// 				entityVersionId: "1",
// 				parentEntityKey: "tenant:tenant1",
// 			},
// 			expectKey: "tenant:tenant1:entity1:123:1",
// 		},
// 		{
// 			name: "Invalid empty entity name",
// 			input: keyInput{
// 				entityName: "",
// 				entityId:   "123",
// 			},
// 			expectError: true,
// 		},
// 		{
// 			name: "Invalid empty entity ID",
// 			input: keyInput{
// 				entityName: "entity1",
// 				entityId:   "",
// 			},
// 			expectError: true,
// 		},
// 		{
// 			name: "Invalid entity name contains reserved namespace key prefix",
// 			input: keyInput{
// 				entityName: "__entity1",
// 				entityId:   "123",
// 			},
// 			expectError: true,
// 		},
// 		{
// 			name: "Invalid entity ID contains reserved namespace key prefix",
// 			input: keyInput{
// 				entityName: "entity1",
// 				entityId:   "__123",
// 			},
// 			expectError: true,
// 		},
// 		{
// 			name: "Invalid entity version ID contains reserved namespace key prefix",
// 			input: keyInput{
// 				entityName:      "entity1",
// 				entityId:        "123",
// 				entityVersionId: "__1",
// 			},
// 			expectError: true,
// 		},
// 		{
// 			name: "Invalid entity parent key contains reserved namespace key prefix",
// 			input: keyInput{
// 				entityName:      "entity1",
// 				entityId:        "123",
// 				parentEntityKey: "__tenant:tenant1",
// 			},
// 			expectError: true,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			key, err := NewEntityKey(
// 				tt.input.entityName,
// 				tt.input.entityId,
// 				tt.input.entityVersionId,
// 				tt.input.parentEntityKey,
// 			)
// 			if tt.expectError && err == nil {
// 				t.Errorf("expected an error but got nil")
// 			}
// 			if !tt.expectError && err != nil {
// 				t.Errorf("unexpected error: %v", err)
// 			}
// 			if key != tt.expectKey {
// 				t.Errorf("expected key: %q, got: %q", tt.expectKey, key)
// 			}
// 		})
// 	}
// }
