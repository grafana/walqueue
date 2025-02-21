package types

import (
	gocontext "context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SyncMailbox_Basic(t *testing.T) {
	t.Parallel()

	m := NewSyncMailbox[string, bool]()
	assert.NotNil(t, m)

	m.Start()

	// Set up receiver
	var received []string
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for cb := range m.ReceiveC() {
			received = append(received, cb.Value)
			cb.Notify(true, nil)
			wg.Done()
		}
	}()

	// Test sending multiple messages
	ctx := gocontext.Background()
	_, err := m.Send(ctx, "hello")
	assert.NoError(t, err)
	_, err = m.Send(ctx, "world")
	assert.NoError(t, err)

	wg.Wait()

	assert.Equal(t, []string{"hello", "world"}, received)
}

func Test_SyncMailbox_Concurrent(t *testing.T) {
	t.Parallel()

	m := NewSyncMailbox[int, int]()
	assert.NotNil(t, m)

	m.Start()

	// Set up receiver
	var received []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	const numSenders = 10
	const messagesPerSender = 100
	wg.Add(numSenders * messagesPerSender)

	go func() {

		for cb := range m.ReceiveC() {
			mu.Lock()
			received = append(received, cb.Value)
			mu.Unlock()
			cb.Notify(cb.Value, nil)
			wg.Done()
		}
	}()

	// Send concurrently

	var sendWg sync.WaitGroup
	sendWg.Add(numSenders)

	for i := 0; i < numSenders; i++ {
		go func(senderID int) {
			defer sendWg.Done()
			for j := 0; j < messagesPerSender; j++ {
				value := senderID*messagesPerSender + j
				ret, err := m.Send(gocontext.Background(), value)
				assert.NoError(t, err)
				assert.Equal(t, ret, value)
			}
		}(i)
	}

	sendWg.Wait()
	wg.Wait()

	assert.Equal(t, numSenders*messagesPerSender, len(received))
}
