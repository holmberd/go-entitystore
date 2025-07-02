# EntityStore

> ⚠️ **Work In Progress**: This project is not yet production ready. APIs and behavior may change. Use at your own risk.

A generic, Redis-backed entity store for Go to efficiently manage and persist entities with flexible key construction and namespacing.

---

## Features
- Fast, in-memory Redis backend (supports [go-redis](https://github.com/go-redis/redis))
- Generic type support for your own entity types
- Flexible, validated key construction with namespaces
- Simple, extensible, and concurrency-safe
- Designed for easy unit and integration testing
- Minimal dependencies, idiomatic Go API

---

## Installation

```sh
go get github.com/holmberd/go-entitystore
```

## Example Usage

```Go
package main

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/holmberd/go-entitystore/datastore"
	"github.com/holmberd/go-entitystore/entitystore"
	"github.com/holmberd/go-entitystore/keyfactory"
)

type User struct {
	Key  string
	ID   string
	Name string
}

func NewUser(
	id string,
	name string,
) (*User, error) {
	u := &User{
		ID:   id,
		Name: name,
	}
	var err error
	u.Key, err = keyfactory.NewEntityKey(keyfactory.EntityKindUser, u.ID, "", "")
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (u User) GetKey() string {
	return u.Key
}

func (u User) MarshalProto() ([]byte, error) {
	return []byte{}, nil
}

func (u *User) UnmarshalProto(data []byte) error {
	return nil
}

type UserStore struct {
	*entitystore.EntityStore[User, *User]
}

func NewUserStore(namespace string, dsClient *datastore.Client) (*UserStore, error) {
	entityStore, err := entitystore.New[User](
		string(keyfactory.EntityKindUser),
		namespace,
		dsClient,
	)
	if err != nil {
		return nil, err
	}
	return &UserStore{
		EntityStore: entityStore,
	}, nil
}

func main() {
	rsClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rsClient.Close()

	dsClient, err := datastore.NewClient(rsClient)
	if err != nil {
		panic(err)
	}
	store, err := NewUserStore("test-1", dsClient)
	if err != nil {
		panic(err)
	}

	var users []User
	data := []struct{ id, name string }{
		{"1", "Alice"},
		{"2", "Sarah"},
		{"3", "Susan"},
	}
	for _, d := range data {
		u, _ := NewUser(d.id, d.name)
		users = append(users, *u)
	}

	// Save entites.
	if _, err := store.AddBatch(context.Background(), users, 0); err != nil {
		panic(err)
	}

	// Retrieve entities
	out, err := store.GetByKeys(
		context.Background(),
		[]string{
			users[0].GetKey(),
			users[1].GetKey(),
			users[2].GetKey(),
		})
	if err != nil {
		panic(err)
	}
	fmt.Println("Users:", out)
}
```

## Test Integration
See `entity_store_suite_test.go` for example.
