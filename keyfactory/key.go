// Package keyfactory provides utilities for constructing structured keys with optional namespacing.
//
// If keys become too long, consider using a hash-based namespace to reduce storage overhead.
package keyfactory

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"

	"github.com/holmberd/go-entitystore/keyfactory/internal/rediskey"
)

const (
	WildcardAnyChar            = rediskey.WildcardAnyChar   // Matches exactly one character.
	WildcardAnyString          = rediskey.WildcardAnyString // Matches zero or more characters.
	ReservedNamespaceDelimiter = "__"                       // Delimiter placed before and after each namespace key.
)

func keyNamespace(ns string) string {
	if ns == "" {
		return ""
	}
	return ReservedNamespaceDelimiter + strings.ToLower(ns) + ReservedNamespaceDelimiter
}

// GenerateRandomKey generates a random 10-character string key.
// The generated string is a valid key fragment.
func GenerateRandomKey() string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	key := make([]byte, 10)
	for i := range key {
		key[i] = letters[rand.Intn(len(letters))]
	}
	return string(key)
}

// Validate validates that the provided string is a valid key fragment.
//
// TODO: Should we expliticly check reserved delimiter, or make it implicit.
func ValidateKeyFragment(f string) error {
	if err := validateKeyFragments(f); err != nil {
		return err
	}
	if err := rediskey.Validate(f); err != nil {
		return err
	}
	return nil
}

// Key represent a fully qualified datastore key.
type Key struct {
	key       string // Logical key.
	namespace string // Key namespace.
}

func NewKey(key string, namespace string) *Key {
	if !strings.HasPrefix(namespace, ReservedNamespaceDelimiter) {
		namespace = keyNamespace(namespace)
	}
	return &Key{key: key, namespace: namespace}
}

func (k *Key) Key() string {
	return k.key
}

func (k *Key) Namespace() string {
	return k.namespace
}

// RedisKey converts a key to a valid Redis key string.
func (k *Key) RedisKey() string {
	return rediskey.Build(k.namespace, k.key)
}

// marshal marshals the key's string representation to the buffer.
func (k *Key) marshal(b *bytes.Buffer) {
	b.WriteString(k.key)
}

// String returns a string representation of the key. It does not include the namespace.
func (k *Key) String() string {
	if k == nil {
		return ""
	}
	b := bytes.NewBuffer(make([]byte, 0, 512))
	k.marshal(b)
	return b.String()
}

// Equal returns whether two keys are equal.
func (k *Key) Equal(o *Key) bool {
	return k == o
}

// KeyBuilder build a fully qualified application storage redis key.
//   - Either key or wildcard must be set.
//
// Key structure: "<__namespace__>:<key>"
type KeyBuilder struct {
	key       string                // Must be a valid Redis key.
	parentKey string                // Must be a valid Redis key.
	wildcard  rediskey.GlobWildcard // For wildcard key matching.
	namespace string                // Optional key namespace.
}

func NewKeyBuilder() *KeyBuilder {
	return &KeyBuilder{}
}

func (b *KeyBuilder) Clone() *KeyBuilder {
	return &KeyBuilder{
		key:       b.key,
		parentKey: b.parentKey,
		wildcard:  b.wildcard,
		namespace: b.namespace,
	}
}

func (b *KeyBuilder) WithKey(key string) {
	b.key = key
}

func (b *KeyBuilder) WithParentKey(key string) {
	b.parentKey = key
}

func (b *KeyBuilder) WithWildcard(wc rediskey.GlobWildcard) {
	b.wildcard = wc
}

func (b *KeyBuilder) WithNamespace(ns string) {
	b.namespace = ns
}

func (b *KeyBuilder) Reset() {
	b.key = ""
	b.parentKey = ""
	b.wildcard = ""
	b.namespace = ""
}

// Build compiles the new key.
func (b *KeyBuilder) Build() (*Key, error) {
	return b.build()
}

// Build compiles the new key and reset the builder state.
func (b *KeyBuilder) BuildAndReset() (*Key, error) {
	defer b.Reset()
	return b.build()
}

func (b *KeyBuilder) build() (*Key, error) {
	key := b.key
	if err := validateOptionalKeys(key, b.parentKey, b.namespace); err != nil {
		return nil, fmt.Errorf("keyfactory: %w", err)
	}
	key = rediskey.Build(b.parentKey, key)
	if b.wildcard != "" {
		key = rediskey.BuildMatchKeyPattern(key, b.wildcard)
	}
	if key == "" {
		return nil, fmt.Errorf("keyfactory: key must not be empty")
	}
	return NewKey(key, b.namespace), nil
}

// KeyBuilderWithNamespace represent a KeyBuilder with a fixed namespace across key constructions.
type KeyBuilderWithNamespace struct {
	*KeyBuilder
}

func NewKeyBuilderWithNamespace(namespace string) *KeyBuilderWithNamespace {
	return &KeyBuilderWithNamespace{KeyBuilder: &KeyBuilder{namespace: namespace}}
}

func (b *KeyBuilderWithNamespace) Clone() *KeyBuilderWithNamespace {
	return &KeyBuilderWithNamespace{
		KeyBuilder: &KeyBuilder{
			key:       b.key,
			parentKey: b.parentKey,
			wildcard:  b.wildcard,
			namespace: b.namespace,
		},
	}
}

func (b *KeyBuilderWithNamespace) Reset() {
	b.key = ""
	b.parentKey = ""
	b.wildcard = ""
	// b.namespace is intentionally not reset.
}

func (b *KeyBuilderWithNamespace) BuildAndReset() (*Key, error) {
	defer b.Reset()
	return b.build()
}

// validateOptionalKeys validates keys and ignores empty keys.
func validateOptionalKeys(keys ...string) error {
	if err := validateKeyFragments(keys...); err != nil {
		return err
	}
	for _, key := range keys {
		if key == "" {
			continue // Skip any empty keys.
		}
		if err := rediskey.Validate(key); err != nil {
			return err
		}
	}
	return nil
}

func validateKeyFragments(keyFragments ...string) error {
	for _, keyFragment := range keyFragments {
		if keyFragment == "" {
			continue // Skip empty fragments.
		}
		if strings.HasPrefix(keyFragment, ReservedNamespaceDelimiter) {
			return fmt.Errorf(
				"key fragment '%s' must not contain reserved namespace key prefix '%s'",
				keyFragment,
				ReservedNamespaceDelimiter,
			)
		}
	}
	return nil
}

// BuildRedisKey combines multiple Redis keys into a single Redis key.
func BuildRedisKey(keys ...string) string {
	return rediskey.Build(keys...)
}
