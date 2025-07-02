package keyfactory

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/holmberd/go-entitystore/keyfactory/internal/rediskey"
)

// ParseRedisKey parses a Redis key into a Key.
//
// Example:
//
//	key, _ := ParseRedisKey("__app1__:tenant:tenant1:product:product-1")
//	// key  => *Key{key: "tenant1:product:product-1", namespace: "__app1__"}
func ParseRedisKey(key string) (*Key, error) {
	if err := rediskey.Validate(key); err != nil {
		return nil, fmt.Errorf("keyfactory: failed to parse redis key '%s': %w", key, err)
	}
	var namespacePattern = regexp.MustCompile(`^(?P<namespace>__\w+__)[:]?`) // https://regex101.com/r/JanbQ8/1
	var namespace string

	// Extract namespace if present.
	if matches := namespacePattern.FindStringSubmatch(key); len(matches) > 0 {
		full := matches[0]
		namespace = matches[1]

		// Trim suffix/prefix since NewKey() applies them.
		namespace = strings.TrimSuffix(
			strings.TrimPrefix(namespace, ReservedNamespaceDelimiter),
			ReservedNamespaceDelimiter,
		)
		key = strings.TrimPrefix(key, full)
	}
	return NewKey(key, namespace), nil
}
