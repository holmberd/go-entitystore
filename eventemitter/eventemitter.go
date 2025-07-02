// Package eventemitter provides functionality for event handling that allows registering
// synchronous or asynchronous listeners and emitting events with arbitrary arguments.
//
// By default each listener is called synchronously when an event is emitted.
// If you want asynchronous (non-blocking) listeners, wrap your listener in a go routine.
//
// Example:
//
//	e := eventemitter.New()
//	token := e.AddListener("my-event", func(args ...any) { fmt.Println(args...) })
//	e.Emit("my-event", 1, 2, 3) // Output: 1 2 3
//	e.RemoveListener("my-event", token)
package eventemitter

import (
	"math/rand"
	"slices"
	"sync"
)

// ListenerToken is the token returned when a listener is added.
type ListenerToken string

func generateToken() ListenerToken {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	key := make([]byte, 6)
	for i := range key {
		key[i] = letters[rand.Intn(len(letters))]
	}
	return ListenerToken(key)
}

// EventTarget instance represents an event target tied to a specific event name.
type EventTarget struct {
	eventEmitter *EventEmitter
	eventName    string
}

func NewEventTarget(eventName string) *EventTarget {
	return &EventTarget{New(), eventName}
}

func (et *EventTarget) EventName() string {
	return et.eventName
}

func (et *EventTarget) AddListener(listener func(args ...any)) ListenerToken {
	return et.eventEmitter.AddListener(et.eventName, listener)
}

func (et *EventTarget) RemoveListener(token ListenerToken) bool {
	return et.eventEmitter.RemoveListener(et.eventName, token)
}

func (et *EventTarget) RemoveAllListeners() bool {
	return et.eventEmitter.RemoveAllListeners(et.eventName)
}

func (et *EventTarget) Emit(args ...any) bool {
	return et.eventEmitter.Emit(et.eventName, args...)
}

// EventEmitter instance instance supports adding multiple named events
// and is safe for concurrent use.
type EventEmitter struct {
	mu     sync.RWMutex
	events map[string][]eventListener
}

type eventListener struct {
	token   ListenerToken
	handler func(args ...any)
}

// New creates a new EventEmitter instance.
func New() *EventEmitter {
	return &EventEmitter{
		events: make(map[string][]eventListener),
	}
}

// AddListener adds a listener function to a specific event.
func (e *EventEmitter) AddListener(eventName string, listener func(args ...any)) ListenerToken {
	e.mu.Lock()
	defer e.mu.Unlock()

	token := generateToken()
	e.events[eventName] = append(e.events[eventName], eventListener{
		token:   token,
		handler: listener,
	})
	return token
}

// RemoveListener removes a listener by token from a specific event.
func (e *EventEmitter) RemoveListener(eventName string, token ListenerToken) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	listeners, ok := e.events[eventName]
	if !ok {
		return false
	}
	for i, listener := range listeners {
		if listener.token == token {
			e.events[eventName] = slices.Delete(listeners, i, i+1)
			return true
		}
	}
	return false
}

// RemoveAllListeners removes all listeners for the specified event.
func (e *EventEmitter) RemoveAllListeners(eventName string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.events[eventName]; ok {
		delete(e.events, eventName)
		return true
	}
	return false
}

// Emit calls each listener synchronously for the given event, passing any provided args.
func (e *EventEmitter) Emit(eventName string, args ...any) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	listeners, ok := e.events[eventName]
	if !ok || len(listeners) == 0 {
		return false
	}
	for _, listener := range listeners {
		listener.handler(args...)
	}
	return true
}
