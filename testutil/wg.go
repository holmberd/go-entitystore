package testutil

import (
	"sync"
	"testing"
	"time"
)

func WaitGroupWithTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait() // Blocks until waitgroup is done.
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(timeout):
		t.Fatalf("timeout after %s waiting for WaitGroup", timeout)
	}
}
