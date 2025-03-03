package filequeue

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func BenchmarkWriteFile(b *testing.B) {
	dir := b.TempDir()
	log := log.NewNopLogger()
	mbx := types.NewMailbox[types.DataHandle]()
	q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
		_ = mbx.Send(ctx, dh)
	}, &fakestats{}, log)
	require.NoError(b, err)
	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()
	q.Start(ctx)
	defer q.Stop()
	// 1 MB blocks
	testData := make([]byte, 1014*1024*1024)
	count := atomic.NewInt32(0)
	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		for range 100 {
			wg.Add(1)
			go func() {
				err = q.Store(context.Background(), nil, testData)
				require.NoError(b, err)
				wg.Done()
				count.Inc()
			}()
		}
		wg.Wait()
	}
	require.NoError(b, err)
	fmt.Printf("written %d MB\n", count.Load())
}
