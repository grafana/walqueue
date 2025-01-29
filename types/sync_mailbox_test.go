package types

import (
	gocontext "context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_SyncMailbox_Basic(t *testing.T) {
	m := NewSyncMailbox[string, bool]()
	assert.NotNil(t, m)

	// Set up receiver
	var received []string
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			cb, ok := <-m.Out()
			if !ok {
				return
			}
			received = append(received, cb.Value)
			cb.Notify(true, nil)
		}
	}()

	// Test sending multiple messages
	ctx := gocontext.Background()
	_, err := m.In(ctx, "hello")
	assert.NoError(t, err)
	_, err = m.In(ctx, "world")
	assert.NoError(t, err)
	m.Close()

	wg.Wait()

	assert.Equal(t, []string{"hello", "world"}, received)
}

func Test_SyncMailbox_Stopped(t *testing.T) {
	m := NewSyncMailbox[string, bool]()
	assert.NotNil(t, m)
	ctx := gocontext.Background()
	ctx, cncl := gocontext.WithTimeout(ctx, 1*time.Second)
	defer cncl()

	success, err := m.In(ctx, "should fail")
	assert.Error(t, err)
	assert.False(t, success)
}
func Test_SyncMailbox_Concurrent(t *testing.T) {
	m := NewSyncMailbox[int, int]()
	assert.NotNil(t, m)

	// Set up receiver
	var received []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			cb, ok := <-m.Out()
			if !ok {
				return
			}
			mu.Lock()
			received = append(received, cb.Value)
			mu.Unlock()
			cb.Notify(cb.Value, nil)
		}
	}()

	// Send concurrently
	const numSenders = 10
	const messagesPerSender = 100
	var sendWg sync.WaitGroup
	sendWg.Add(numSenders)

	for i := 0; i < numSenders; i++ {
		go func(senderID int) {
			defer sendWg.Done()
			for j := 0; j < messagesPerSender; j++ {
				value := senderID*messagesPerSender + j
				ret, err := m.In(gocontext.Background(), value)
				assert.NoError(t, err)
				assert.Equal(t, ret, value)
			}
		}(i)
	}
	sendWg.Wait()
	m.Close()
	wg.Wait()

	assert.Equal(t, numSenders*messagesPerSender, len(received))
}
