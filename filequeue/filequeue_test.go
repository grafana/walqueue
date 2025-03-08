package filequeue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	"github.com/stretchr/testify/require"
)

func TestFileQueue(t *testing.T) {
	t.Run("DiskStorage", func(t *testing.T) {
		dir := t.TempDir()
		log := log.NewNopLogger()
		mbx := types.NewMailbox[types.DataHandle]()
		q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
			_ = mbx.Send(ctx, dh)
		}, &fakestats{}, log)
		require.NoError(t, err)
		ctx, cncl := context.WithCancel(context.Background())
		defer cncl()
		q.Start(ctx)
		defer q.Stop()
		err = q.Store(context.Background(), nil, []byte("test"))

		require.NoError(t, err)

		meta, buf, err := getHandle(t, mbx)
		require.NoError(t, err)
		require.True(t, string(buf) == "test")
		require.Len(t, meta, 1)
		require.True(t, meta["file_id"] == "1")

		// Ensure nothing new comes through.
		timer := time.NewTicker(100 * time.Millisecond)
		select {
		case <-timer.C:
			return
		case <-mbx.ReceiveC():
			require.True(t, false)
		}
	})

	t.Run("MemoryStorage", func(t *testing.T) {
		dir := t.TempDir()
		log := log.NewNopLogger()
		mbx := types.NewMailbox[types.DataHandle]()
		q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
			_ = mbx.Send(ctx, dh)
		}, &fakestats{}, log, WithStorageType(StorageMemory))
		require.NoError(t, err)
		ctx, cncl := context.WithCancel(context.Background())
		defer cncl()
		q.Start(ctx)
		defer q.Stop()
		err = q.Store(context.Background(), nil, []byte("test"))

		require.NoError(t, err)

		meta, buf, err := getHandle(t, mbx)
		require.NoError(t, err)
		require.True(t, string(buf) == "test")
		require.Len(t, meta, 1)
		require.True(t, meta["file_id"] == "1")

		// Ensure nothing new comes through.
		timer := time.NewTicker(100 * time.Millisecond)
		select {
		case <-timer.C:
			return
		case <-mbx.ReceiveC():
			require.True(t, false)
		}
	})
}

func TestMetaFileQueue(t *testing.T) {
	dir := t.TempDir()
	log := log.NewNopLogger()
	mbx := types.NewMailbox[types.DataHandle]()

	q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
		_ = mbx.Send(ctx, dh)
	}, &fakestats{}, log)
	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()
	q.Start(ctx)
	defer q.Stop()
	require.NoError(t, err)
	err = q.Store(context.Background(), map[string]string{"name": "bob"}, []byte("test"))
	require.NoError(t, err)

	meta, buf, err := getHandle(t, mbx)
	require.NoError(t, err)
	require.True(t, string(buf) == "test")
	require.Len(t, meta, 2)
	require.True(t, meta["name"] == "bob")
}

func TestCorruption(t *testing.T) {
	// This test needs direct disk access so we only run it with disk storage
	dir := t.TempDir()
	log := log.NewNopLogger()
	mbx := types.NewMailbox[types.DataHandle]()

	// Create custom queue with disk filesystem explicitly
	diskFs := NewDiskFS()
	q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
		_ = mbx.Send(ctx, dh)
	}, &fakestats{}, log, WithCustomFS(diskFs))
	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()
	q.Start(ctx)
	defer q.Stop()
	require.NoError(t, err)

	err = q.Store(context.Background(), map[string]string{"name": "bob"}, []byte("first"))
	require.NoError(t, err)
	err = q.Store(context.Background(), map[string]string{"name": "bob"}, []byte("second"))
	require.NoError(t, err)

	// Send is async so may need to wait a bit for it happen.
	require.Eventually(t, func() bool {
		// First should be 1.committed - using direct disk access for testing
		_, errStat := os.Stat(filepath.Join(dir, "1.committed"))
		return errStat == nil
	}, 2*time.Second, 100*time.Millisecond)

	// Corrupt the file directly using OS (bypassing our filesystem interface)
	filePath := filepath.Join(dir, "1.committed")
	err = os.WriteFile(filePath, []byte("bad"), 0644)
	require.NoError(t, err)

	// First handle should error due to the corrupted file
	_, _, err = getHandle(t, mbx)
	require.Error(t, err)

	// Second handle should be fine
	meta, buf, err := getHandle(t, mbx)
	require.NoError(t, err)
	require.True(t, string(buf) == "second")
	require.Len(t, meta, 2)
}

func TestFileDeleted(t *testing.T) {
	// This test needs direct disk access so we only run it with disk storage
	dir := t.TempDir()
	log := log.NewNopLogger()
	mbx := types.NewMailbox[types.DataHandle]()

	// Create custom queue with disk filesystem explicitly
	diskFs := NewDiskFS()
	q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
		_ = mbx.Send(ctx, dh)
	}, &fakestats{}, log, WithCustomFS(diskFs))
	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()
	q.Start(ctx)
	defer q.Stop()
	require.NoError(t, err)

	evenHandles := make([]string, 0)
	for i := 0; i < 10; i++ {
		err = q.Store(context.Background(), map[string]string{"name": "bob"}, []byte(strconv.Itoa(i)))

		require.NoError(t, err)
		if i%2 == 0 {
			evenHandles = append(evenHandles, filepath.Join(dir, strconv.Itoa(i+1)+".committed"))
		}
	}

	// Send is async so may need to wait a bit for it happen, check for the last file written.
	// Use direct OS access for testing
	require.Eventually(t, func() bool {
		_, errStat := os.Stat(filepath.Join(dir, "10.committed"))
		return errStat == nil
	}, 2*time.Second, 100*time.Millisecond)

	// Manually delete even files from outside the filesystem interface
	for _, h := range evenHandles {
		_ = os.Remove(h)
	}
	
	// Every even file was deleted and should have an error.
	for i := 0; i < 10; i++ {
		_, buf2, err := getHandle(t, mbx)
		if i%2 == 0 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.True(t, string(buf2) == strconv.Itoa(i))
		}
	}
}

func TestOtherFiles(t *testing.T) {
	// This test needs direct disk access so we only run it with disk storage
	dir := t.TempDir()
	log := log.NewNopLogger()
	mbx := types.NewMailbox[types.DataHandle]()

	// Create custom queue with disk filesystem explicitly
	diskFs := NewDiskFS()
	q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
		_ = mbx.Send(ctx, dh)
	}, &fakestats{}, log, WithCustomFS(diskFs))
	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()
	q.Start(ctx)
	defer q.Stop()
	require.NoError(t, err)

	err = q.Store(context.Background(), nil, []byte("first"))
	require.NoError(t, err)
	
	// Create an unrelated file directly in the filesystem
	os.Create(filepath.Join(dir, "otherfile"))
	
	_, buf, err := getHandle(t, mbx)
	require.NoError(t, err)
	require.True(t, string(buf) == "first")
}

func TestResuming(t *testing.T) {
	t.Run("DiskStorage", func(t *testing.T) {
		// Test resuming with disk storage
		dir := t.TempDir()
		log := log.NewNopLogger()
		mbx := types.NewMailbox[types.DataHandle]()

		q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
			_ = mbx.Send(ctx, dh)
		}, &fakestats{}, log)
		ctx, cncl := context.WithCancel(context.Background())
		defer cncl()
		q.Start(ctx)
		require.NoError(t, err)

		err = q.Store(context.Background(), nil, []byte("first"))
		require.NoError(t, err)

		err = q.Store(context.Background(), nil, []byte("second"))
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
		q.Stop()

		mbx2 := types.NewMailbox[types.DataHandle]()

		q2, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
			_ = mbx2.Send(ctx, dh)
		}, &fakestats{}, log)
		require.NoError(t, err)

		q2.Start(ctx)
		defer q2.Stop()
		err = q2.Store(context.Background(), nil, []byte("third"))

		require.NoError(t, err)
		_, buf, err := getHandle(t, mbx2)
		require.NoError(t, err)
		require.True(t, string(buf) == "first")

		_, buf, err = getHandle(t, mbx2)
		require.NoError(t, err)
		require.True(t, string(buf) == "second")

		_, buf, err = getHandle(t, mbx2)
		require.NoError(t, err)
		require.True(t, string(buf) == "third")
	})
	
	t.Run("MemoryStorage", func(t *testing.T) {
		// Memory storage doesn't persist between queue instances
		dir := t.TempDir()
		log := log.NewNopLogger()
		mbx := types.NewMailbox[types.DataHandle]()

		q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
			_ = mbx.Send(ctx, dh)
		}, &fakestats{}, log, WithStorageType(StorageMemory))
		ctx, cncl := context.WithCancel(context.Background())
		defer cncl()
		q.Start(ctx)
		require.NoError(t, err)

		err = q.Store(context.Background(), nil, []byte("first"))
		require.NoError(t, err)

		err = q.Store(context.Background(), nil, []byte("second"))
		require.NoError(t, err)
		
		// Get the records from the first queue
		meta1, buf1, err := getHandle(t, mbx)
		require.NoError(t, err)
		require.True(t, string(buf1) == "first")
		require.Contains(t, meta1, "file_id")
		
		meta2, buf2, err := getHandle(t, mbx)
		require.NoError(t, err)
		require.True(t, string(buf2) == "second")
		require.Contains(t, meta2, "file_id")
		
		// Stop the first queue
		time.Sleep(1 * time.Second)
		q.Stop()

		mbx2 := types.NewMailbox[types.DataHandle]()

		// Create a second queue with in-memory storage
		// This should have a fresh memory with no previous files
		q2, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
			_ = mbx2.Send(ctx, dh)
		}, &fakestats{}, log, WithStorageType(StorageMemory))
		require.NoError(t, err)

		q2.Start(ctx)
		defer q2.Stop()
		
		// The memory files are not persistent, so only the new record should be returned
		err = q2.Store(context.Background(), nil, []byte("third"))
		require.NoError(t, err)
		
		// Only "third" should be in the new queue
		meta3, buf3, err := getHandle(t, mbx2)
		require.NoError(t, err)
		require.Equal(t, "third", string(buf3))
		require.Contains(t, meta3, "file_id")
		require.Equal(t, "1", meta3["file_id"]) // ID should be 1 again since this is a fresh instance
		
		// Verify there's nothing else in the queue
		timer := time.NewTicker(100 * time.Millisecond)
		select {
		case <-timer.C:
			return
		case <-mbx2.ReceiveC():
			require.Fail(t, "Unexpected data in memory queue")
		}
	})
}

func TestInMemoryResilience(t *testing.T) {
	dir := "/virtual/dir" // This directory doesn't exist on disk
	log := log.NewNopLogger()
	mbx := types.NewMailbox[types.DataHandle]()

	// Using in-memory storage should work even if the directory doesn't exist
	q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
		_ = mbx.Send(ctx, dh)
	}, &fakestats{}, log, WithStorageType(StorageMemory))
	require.NoError(t, err)
	
	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()
	q.Start(ctx)
	defer q.Stop()
	
	// Write a large number of records to the in-memory filesystem
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		err = q.Store(ctx, map[string]string{"index": strconv.Itoa(i)}, data)
		require.NoError(t, err)
	}
	
	// Read back the records and verify
	for i := 0; i < 100; i++ {
		meta, buf, err := getHandle(t, mbx)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("data-%d", i), string(buf))
		require.Equal(t, strconv.Itoa(i), meta["index"])
	}
}

func getHandle(t *testing.T, mbx *types.Mailbox[types.DataHandle]) (map[string]string, []byte, error) {
	timer := time.NewTicker(5 * time.Second)
	select {
	case <-timer.C:
		require.True(t, false)
		// This is only here to satisfy the linting.
		return nil, nil, nil
	case item, ok := <-mbx.ReceiveC():
		require.True(t, ok)
		return item.Pop()
	}
}

var _ types.StatsHub = (*fakestats)(nil)

type fakestats struct {
}

func (fs fakestats) SendParralelismStats(stats types.ParralelismStats) {

}

func (fs fakestats) RegisterParralelism(f func(types.ParralelismStats)) types.NotificationRelease {
	return func() {

	}
}

func (fakestats) Start(_ context.Context) {
}
func (fakestats) Stop() {
}
func (fs *fakestats) SendSeriesNetworkStats(ns types.NetworkStats) {
}
func (fakestats) SendSerializerStats(_ types.SerializerStats) {
}
func (fakestats) SendMetadataNetworkStats(_ types.NetworkStats) {
}
func (fakestats) RegisterSeriesNetwork(_ func(types.NetworkStats)) (_ types.NotificationRelease) {
	return func() {}
}
func (fakestats) RegisterMetadataNetwork(_ func(types.NetworkStats)) (_ types.NotificationRelease) {
	return func() {}
}
func (fakestats) RegisterSerializer(_ func(types.SerializerStats)) (_ types.NotificationRelease) {
	return func() {}
}
