package eventemitter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holmberd/go-entitystore/testutil"
	"github.com/stretchr/testify/assert"
)

func TestEventEmitter(t *testing.T) {
	t.Run("Add listener and emit event", func(t *testing.T) {
		e := New()
		lowerCaseEvent := "my-event"
		upperCaseEvent := "My-Event"
		var called1, called2 bool

		token := e.AddListener(lowerCaseEvent, func(args ...any) { called1 = true })
		assert.NotZero(t, token, "should return a valid token")
		result := e.Emit(lowerCaseEvent)
		assert.True(t, result, "should return true if listeners are triggered")
		assert.True(t, called1, "should have called listener")
		called1 = false

		token = e.AddListener("My-Event", func(args ...any) { called2 = true })
		assert.NotZero(t, token, "should return a valid token")
		result = e.Emit(upperCaseEvent)
		assert.True(t, result, "should return true if listeners are triggered")
		assert.True(t, called2, "should call listener")
		assert.False(t, called1, "should not have called other listener again")
	})

	t.Run("Emit event with arguments", func(t *testing.T) {
		e := New()
		var receivedArgs []interface{}
		e.AddListener("arg-event", func(args ...interface{}) {
			receivedArgs = args
		})

		e.Emit("arg-event", 123, "hello", true)
		assert.Len(t, receivedArgs, 3)
		assert.Equal(t, 123, receivedArgs[0])
		assert.Equal(t, "hello", receivedArgs[1])
		assert.Equal(t, true, receivedArgs[2])
	})

	t.Run("Emit event with no listeners", func(t *testing.T) {
		e := New()
		result := e.Emit("no-listeners")
		assert.False(t, result, "should return false if no listeners exist")
	})

	t.Run("Add multiple listener and emit event", func(t *testing.T) {
		e := New()
		eventName := "my-event"
		count := 0
		t1 := e.AddListener(eventName, func(args ...any) { count++ })
		t2 := e.AddListener(eventName, func(args ...any) { count++ })
		assert.NotZero(t, t1, "should return a valid token")
		assert.NotZero(t, t2, "should return a valid token")

		ok := e.Emit("my-event")
		assert.True(t, ok, "should return true if listeners are triggered")
		assert.Equal(t, 2, count, "should call both listeners")
	})

	t.Run("Remove existing listener", func(t *testing.T) {
		e := New()
		eventName := "remove-me"
		called := false
		token := e.AddListener(eventName, func(args ...interface{}) {
			called = true
		})

		removed := e.RemoveListener(eventName, token)
		assert.True(t, removed, "should successfully remove listener")

		e.Emit(eventName)
		assert.False(t, called, "should not call listener after removal")
	})

	t.Run("Remove non-existent listener", func(t *testing.T) {
		e := New()
		removed := e.RemoveListener("missing-event", "non-existent-token")
		assert.False(t, removed, "should return false when removing non-existent listener")
	})

	t.Run("Remove all existing listeners", func(t *testing.T) {
		e := New()
		e.AddListener("cleanup", func(args ...interface{}) {})
		e.AddListener("cleanup", func(args ...interface{}) {})

		ok := e.RemoveAllListeners("cleanup")
		assert.True(t, ok, "should return true when listeners are removed")

		result := e.Emit("cleanup")
		assert.False(t, result, "should return false after all listeners are removed")
	})

	t.Run("Remove all non-existent listeners", func(t *testing.T) {
		e := New()
		ok := e.RemoveAllListeners("ghost")
		assert.False(t, ok, "should return false when trying to remove from an empty event")
	})

	t.Run("Emit concurrent events", func(t *testing.T) {
		e := New()
		const numListeners = 100
		const numEmitters = 50
		var wg sync.WaitGroup
		var called atomic.Int32

		for i := 0; i < numListeners; i++ {
			e.AddListener("tick", func(args ...any) {
				called.Add(1)
			})
		}
		for i := 0; i < numEmitters; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				e.Emit("tick")
			}()
		}
		testutil.WaitGroupWithTimeout(t, &wg, time.Second)
		assert.Equal(t, numListeners*numEmitters, int(called.Load()), "should have called all listeners for each emit")
	})

	t.Run("Handle events with asynchronous listeners", func(t *testing.T) {
		e := New()
		const numListeners = 100
		var wg sync.WaitGroup
		var called atomic.Int32

		for i := 0; i < numListeners; i++ {
			e.AddListener("tick", func(args ...any) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					called.Add(1)
				}()
			})
		}
		e.Emit("tick")
		testutil.WaitGroupWithTimeout(t, &wg, time.Second)
		assert.Equal(t, numListeners, int(called.Load()), "should have called each asynchronous listener")
	})
}

func TestEventTarget(t *testing.T) {
	t.Run("Return event name", func(t *testing.T) {
		name := "my-event"
		et := NewEventTarget(name)
		assert.Equal(t, name, et.EventName(), "should return correct event name")
	})

	t.Run("Add listener and emit event", func(t *testing.T) {
		et := NewEventTarget("test-event")
		called := false
		token := et.AddListener(func(args ...any) {
			called = true
		})
		assert.NotZero(t, token, "should return a valid token")
		ok := et.Emit()
		assert.True(t, ok, "should return true when listener is triggered")
		assert.True(t, called, "listener should call listener")
	})

	t.Run("Add multiple listeners and emit event", func(t *testing.T) {
		et := NewEventTarget("multi")
		count := 0
		et.AddListener(func(args ...any) { count++ })
		et.AddListener(func(args ...any) { count++ })
		ok := et.Emit()
		assert.True(t, ok)
		assert.Equal(t, 2, count, "should call both listeners")
	})

	t.Run("Emit events with arguments", func(t *testing.T) {
		et := NewEventTarget("args-event")
		var received []any
		et.AddListener(func(args ...any) {
			received = args
		})
		ok := et.Emit(42, "foo", true)
		assert.True(t, ok, "should return true when listener is triggered")
		assert.Equal(t, []any{42, "foo", true}, received, "should receive all arguments")
	})

	t.Run("Emit event with no listeners", func(t *testing.T) {
		et := NewEventTarget("empty")
		ok := et.Emit("noop")
		assert.False(t, ok, "Emit should return false if no listeners are registered")
	})

	t.Run("Remove existing listener", func(t *testing.T) {
		et := NewEventTarget("removable")
		called := false
		token := et.AddListener(func(args ...any) {
			called = true
		})
		removed := et.RemoveListener(token)
		assert.True(t, removed, "should remove the listener")
		et.Emit("test")
		assert.False(t, called, "should not call listener after removal")
	})

	t.Run("Remove all existing listeners", func(t *testing.T) {
		et := NewEventTarget("wipe")
		et.AddListener(func(args ...any) {})
		et.AddListener(func(args ...any) {})

		ok := et.RemoveAllListeners()
		assert.True(t, ok, "should remove all listeners")

		ok = et.Emit()
		assert.False(t, ok, "should not emit after removing all listeners")
	})
}
