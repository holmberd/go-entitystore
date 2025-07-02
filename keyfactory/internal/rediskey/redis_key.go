package rediskey

import (
	"fmt"
	"regexp"
	"strings"
)

type GlobWildcard string

const (
	WildcardAnyChar      GlobWildcard = "?"  // Matches exactly one character.
	WildcardAnyString    GlobWildcard = "*"  // Matches zero or more characters.
	KeyFragmentDelimiter              = ":"  // Standard Redis delimiter.
	keyMaxLength                      = 1024 // Practical limit (avoid large keys).
)

var redisKeyRegex = regexp.MustCompile(`^[a-zA-Z0-9:_\-\*\?\[\]\(\),]+$`) // Allowed Redis key characters.

type InvalidRedisKeyError string

func (e InvalidRedisKeyError) Error() string { return "invalid redis key: " + string(e) }

// New constructs a valid Redis key string from the provided key fragments.
//
// Keys follow a structured format: <keyFragment1>:<keyFragment2>:...:<keyFragmentN>
//   - Empty key fragments are ignored.
//   - Key fragments are converted to lowercase strings.
//   - A key fragment containing ":" will result in an error to prevent unintended key structure issues.
//
// Example:
//
//	key, err := New("entity", "123")
//	fmt.Println(key) // "entity:123"
func New(keyFragments ...string) (string, error) {
	// Validate key fragments.
	keys := make([]string, 0, len(keyFragments))
	for _, fragment := range keyFragments {
		if fragment == "" {
			continue // Skip empty fragments.
		}
		if strings.Contains(fragment, KeyFragmentDelimiter) {
			return "", InvalidRedisKeyError(
				fmt.Sprintf("key fragment '%s' must not contain delimiter '%s", fragment, KeyFragmentDelimiter),
			)
		}
		keys = append(keys, strings.ToLower(fragment))
	}
	key := Build(keys...)
	err := Validate(key)
	if err != nil {
		return "", err
	}
	return key, nil
}

// Validate validates the provided Redis key.
func Validate(key string) error {
	if key == "" {
		return InvalidRedisKeyError("key must not be empty")
	}
	if len(key) > keyMaxLength {
		return InvalidRedisKeyError(fmt.Sprintf("key '%s' exceeds %d characters", key, keyMaxLength))
	}
	if !redisKeyRegex.MatchString(key) {
		return InvalidRedisKeyError(fmt.Sprintf("key '%s' contains invalid characters", key))
	}
	if strings.HasPrefix(key, KeyFragmentDelimiter) || strings.HasSuffix(key, KeyFragmentDelimiter) {
		return InvalidRedisKeyError(
			fmt.Sprintf("key '%s' must not start or end with '%s'", key, KeyFragmentDelimiter),
		)
	}
	return nil
}

// Build joins multiple valid Redis keys to produce a single complete Redis key string.
// It skips any empty keys.
func Build(keys ...string) string {
	var keyBuilder strings.Builder
	first := true
	for _, k := range keys {
		if k == "" {
			continue
		}
		if !first {
			keyBuilder.WriteString(KeyFragmentDelimiter)
		}
		keyBuilder.WriteString(k)
		first = false
	}
	return keyBuilder.String()
}

// Parse splits a Redis key into its individual segments seperated by the standard Redis delimiter.
func Parse(key string) []string {
	return strings.Split(key, KeyFragmentDelimiter)
}

// BuildMatchKeyPattern builds a Redis glob match pattern from a valid base Redis key.
//
// Example:
//
//	baseKey, _ := New("namespace:entity")
//	fmt.Println(BuildMatchKeyPattern(baseKey, WildcardAnyString) // "namespace:entity:*"
func BuildMatchKeyPattern(baseKey string, wildcard GlobWildcard) string {
	return fmt.Sprintf("%s%s%s", baseKey, KeyFragmentDelimiter, wildcard)
}
