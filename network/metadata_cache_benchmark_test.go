package network

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/atomic"

	"github.com/grafana/walqueue/types"
	v2 "github.com/grafana/walqueue/types/v2"
)

// BenchmarkMetadataCacheConcurrency benchmarks the cache when there are only many concurrent readers
func BenchmarkMetadataCacheReadOnly(b *testing.B) {
	concurrencyLevels := []int{1, 10, 50, 100, 500, 1000}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("goroutines_%d", concurrency), func(b *testing.B) {
			benchmarkMetadataCacheConcurrency(b, concurrency, false)
		})
	}
}

// BenchmarkMetadataCacheConcurrency benchmarks cache under high concurrency
func BenchmarkMetadataCacheConcurrency(b *testing.B) {
	concurrencyLevels := []int{1, 10, 50, 100, 500, 1000}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("goroutines_%d", concurrency), func(b *testing.B) {
			benchmarkMetadataCacheConcurrency(b, concurrency, true)
		})
	}
}

func benchmarkMetadataCacheConcurrency(b *testing.B, goroutines int, withWriting bool) {
	cache, err := NewMetadataCache(1000)
	if err != nil {
		b.Fatal(err)
	}

	metricsToTestWith := 10000
	testMetadata, metricNames := createTestMetadatums(metricsToTestWith)
	var wg sync.WaitGroup
	doneWriting := atomic.NewBool(false)

	if withWriting {
		// Pre-populate cache
		for i := range 1000 {
			cache.Set(testMetadata[i])
		}

		// One goroutine should be adding metadata continuously
		go func() {
			r := rand.New(rand.NewSource(time.Now().UnixMilli()))
			for doneWriting.Load() == false {
				cache.Set(testMetadata[r.Intn(metricsToTestWith)])
			}
		}()
	}

	b.ResetTimer()

	// N worker goroutines performing read operations
	for i := range goroutines {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(workerID)))

			for range b.N {
				// Read operation
				cache.GetIfNotSent(metricNames[r.Intn(10000)])
			}
		}(i)
	}

	wg.Wait()
	doneWriting.Store(true)
}

func createTestMetadatums(count int) ([]types.MetadataDatum, []string) {
	metricNames := make([]string, count)
	for i := range metricNames {
		metricNames[i] = fmt.Sprintf("metric_%d", i)
	}
	testMetadata := make([]types.MetadataDatum, count)
	for i := range testMetadata {
		testMetadata[i] = createTestMetadata(metricNames[i])
	}

	return testMetadata, metricNames
}

// Helper functions
func createTestMetadata(metricName string) types.MetadataDatum {
	metadata := &prompb.MetricMetadata{
		MetricFamilyName: metricName,
		Type:             prompb.MetricMetadata_COUNTER,
		Help:             fmt.Sprintf("Help text for %s", metricName),
		Unit:             "seconds",
	}

	data, _ := metadata.Marshal()
	return v2.Metadata{
		Buf: data,
	}
}
