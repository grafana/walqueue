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
	defer m.Stop()

	// Set up receiver
	var received []string
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for cb := range m.ReceiveC() {
			received = append(received, cb.Value)
			cb.Notify(true, nil)
		}
	}()

	// Test sending multiple messages
	ctx := gocontext.Background()
	_, err := m.Send(ctx, "hello")
	assert.NoError(t, err)
	_, err = m.Send(ctx, "world")
	assert.NoError(t, err)

	m.Stop()
	wg.Wait()

	assert.Equal(t, []string{"hello", "world"}, received)
}

func Test_SyncMailbox_Stopped(t *testing.T) {
	t.Parallel()

	m := NewSyncMailbox[string, bool]()
	assert.NotNil(t, m)

	m.Start()
	m.Stop()
	success, err := m.Send(gocontext.Background(), "should fail")
	assert.Error(t, err)
	assert.False(t, success)
}

func Test_SyncMailbox_StopBeforeSend(t *testing.T) {
	t.Parallel()

	m := NewSyncMailbox[string, bool]()
	assert.NotNil(t, m)

	m.Stop()
	// Don't start the mailbox at all
	_, err := m.Send(gocontext.Background(), "should fail")
	assert.Error(t, err)
}

func Test_SyncMailbox_Concurrent(t *testing.T) {
	t.Parallel()

	m := NewSyncMailbox[int, int]()
	assert.NotNil(t, m)

	m.Start()
	defer m.Stop()

	// Set up receiver
	var received []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for cb := range m.ReceiveC() {
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
				ret, err := m.Send(gocontext.Background(), value)
				assert.NoError(t, err)
				assert.Equal(t, ret, value)
			}
		}(i)
	}

	sendWg.Wait()
	m.Stop()
	wg.Wait()

	assert.Equal(t, numSenders*messagesPerSender, len(received))
}
