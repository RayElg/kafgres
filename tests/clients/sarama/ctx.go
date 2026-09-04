package main

import (
	"context"
	"time"
)

// A context cancelled when `done` closes: ConsumerGroup.Consume returns on context
// cancellation, and the handler is what knows when enough messages have arrived.
func newCancelOn(done <-chan struct{}) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	go func() {
		<-done
		cancel()
	}()
	return ctx
}
