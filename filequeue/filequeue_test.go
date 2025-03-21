package filequeue

import (
	"context"
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

	err = q.Store(context.Background(), map[string]string{"name": "bob"}, []byte("first"))
	require.NoError(t, err)
	err = q.Store(context.Background(), map[string]string{"name": "bob"}, []byte("second"))

	require.NoError(t, err)

	// Send is async so may need to wait a bit for it happen.
	require.Eventually(t, func() bool {
		// First should be 1.committed
		_, errStat := os.Stat(filepath.Join(dir, "1.committed"))
		return errStat == nil
	}, 2*time.Second, 100*time.Millisecond)

	fi, err := os.Stat(filepath.Join(dir, "1.committed"))

	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dir, fi.Name()), []byte("bad"), 0644)
	require.NoError(t, err)

	_, _, err = getHandle(t, mbx)
	require.Error(t, err)

	meta, buf, err := getHandle(t, mbx)
	require.NoError(t, err)
	require.True(t, string(buf) == "second")
	require.Len(t, meta, 2)
}

func TestFileDeleted(t *testing.T) {
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

	evenHandles := make([]string, 0)
	for i := 0; i < 10; i++ {
		err = q.Store(context.Background(), map[string]string{"name": "bob"}, []byte(strconv.Itoa(i)))

		require.NoError(t, err)
		if i%2 == 0 {
			evenHandles = append(evenHandles, filepath.Join(dir, strconv.Itoa(i+1)+".committed"))
		}
	}

	// Send is async so may need to wait a bit for it happen, check for the last file written.
	require.Eventually(t, func() bool {
		_, errStat := os.Stat(filepath.Join(dir, "10.committed"))
		return errStat == nil
	}, 2*time.Second, 100*time.Millisecond)

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

	err = q.Store(context.Background(), nil, []byte("first"))
	require.NoError(t, err)
	os.Create(filepath.Join(dir, "otherfile"))
	_, buf, err := getHandle(t, mbx)
	require.NoError(t, err)
	require.True(t, string(buf) == "first")
}

func TestResuming(t *testing.T) {
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
