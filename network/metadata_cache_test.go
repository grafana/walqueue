package network

import (
	"strconv"
	"sync"
	"testing"
)

// Benchmark_GetIfNotSent benchmarks concurrent access of an empty cache.
func Benchmark_GetIfNotSent(b *testing.B) {
	goroutineCounts := []int{1, 50, 100, 250, 500, 750, 1000}
	numberOfExecutions := 1000
	for _, n := range goroutineCounts {
		b.Run("goroutines_"+strconv.Itoa(n), func(b *testing.B) {
			cache, _ := NewMetadataCache(10000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(n)
				for j := 0; j < n; j++ {
					key := "key" + strconv.Itoa(j)
					go func() {
						defer wg.Done()
						for range numberOfExecutions {
							cache.GetIfNotSent(key)
						}
					}()
				}
				wg.Wait()
			}
		})
	}
}
