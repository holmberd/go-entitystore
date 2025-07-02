package rediskey

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRedisKey(t *testing.T) {
	tests := []struct {
		name         string
		keyFragments []string
		expectKey    string
		expectError  bool
	}{
		{
			name:         "Single key fragment",
			keyFragments: []string{"single"},
			expectKey:    "single",
		},
		{
			name:         "Mix of empty and non-empty key fragments",
			keyFragments: []string{"", "resource", "1234", ""},
			expectKey:    "resource:1234",
		},
		{
			name:         "Multiple key fragments",
			keyFragments: []string{"resource", "1234"},
			expectKey:    "resource:1234",
		},
		{
			name:         "Uppercase key fragments",
			keyFragments: []string{"Resource", "1234"},
			expectKey:    "resource:1234",
		},
		{
			name:         "Math any string glob pattern",
			keyFragments: []string{"Resource", string(WildcardAnyString)},
			expectKey:    fmt.Sprintf("resource:%s", string(WildcardAnyString)),
		},
		{
			name:         "Math any character glob pattern",
			keyFragments: []string{fmt.Sprintf("resource-%s", string(WildcardAnyChar))},
			expectKey:    fmt.Sprintf("resource-%s", string(WildcardAnyChar)),
		},
		{
			name:         "Empty key fragments",
			keyFragments: []string{},
			expectError:  true,
		},
		{
			name:         "Key fragment contains colon",
			keyFragments: []string{"resource", "123:4"},
			expectError:  true,
		},
		{
			name:         "Key starts with a colon",
			keyFragments: []string{":leading"},
			expectError:  true,
		},
		{
			name:         "Key ends with a colon",
			keyFragments: []string{"trailing:"},
			expectError:  true,
		},
		{
			name:         "Multiple key fragments contain colons",
			keyFragments: []string{"resource", "123:4"},
			expectError:  true,
		},
		{
			name:         "Key fragment contains invalid characters",
			keyFragments: []string{"re@source!", "1234"},
			expectError:  true,
		},
		{
			name:         "Key fragment contains spaces",
			keyFragments: []string{" resource", "123 "},
			expectError:  true,
		},
		{
			name:         "Key exceeds maximum length",
			keyFragments: []string{strings.Repeat("a", keyMaxLength+1)},
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := New(tt.keyFragments...)
			if tt.expectError && err == nil {
				t.Errorf("expected an error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if key != tt.expectKey {
				t.Errorf("expected key: %q, got: %q", tt.expectKey, key)
			}
		})
	}
}

func TestRedisBuildMatchKeyPattern(t *testing.T) {
	tests := []struct {
		keyFragment string
		expect      string
	}{
		{"resource", "resource:*"},
		{"user:123", "user:123:*"},
		{"", ":*"},
	}
	for _, tt := range tests {
		t.Run(tt.keyFragment, func(t *testing.T) {
			got := BuildMatchKeyPattern(tt.keyFragment, WildcardAnyString)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func TestBuildRedisKey(t *testing.T) {
	tests := []struct {
		keys   []string
		expect string
	}{
		{[]string{"a", "b", "c"}, "a:b:c"},
		{[]string{"single"}, "single"},
		{[]string{}, ""},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.keys, ","), func(t *testing.T) {
			got := Build(tt.keys...)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func TestParseRedisKey(t *testing.T) {
	tests := []struct {
		key    string
		expect []string
	}{
		{"a:b:c", []string{"a", "b", "c"}},
		{"single", []string{"single"}},
		{"", []string{""}},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got := Parse(tt.key)
			assert.Equal(t, tt.expect, got)
		})
	}
}
