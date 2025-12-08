package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"

	"github.com/go-kit/log"
	prom_impl "github.com/grafana/walqueue/implementations/prometheus"
	"github.com/grafana/walqueue/types"
	prometheusClient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// jitterServer creates a test HTTP server that responds with a jitter between minDelay and maxDelay
// This simulates variable network conditions
func jitterServer(t testing.TB, minDelayMs, maxDelayMs int, requestCounter *atomic.Int64, totalSignals *atomic.Int64) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Track total requests
		requestCounter.Add(1)
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		data, err = snappy.Decode(nil, data)
		require.NoError(t, err)

		var req prompb.WriteRequest
		err = req.Unmarshal(data)
		require.NoError(t, err)
		totalSignals.Add(int64(len(req.Timeseries)))

		// Introduce random delay between minDelay and maxDelay
		jitter := minDelayMs + rand.Intn(maxDelayMs-minDelayMs)
		time.Sleep(time.Duration(jitter) * time.Millisecond)

		// Return success
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
	})

	return server
}

// createTempDir creates a temporary directory for the WAL files
func createTempDir(t testing.TB) string {
	tempDir := t.TempDir()
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	return tempDir
}

// benchmarkResults holds the results of a benchmark run
type benchmarkResults struct {
	TestName             string  `json:"test_name"`
	BatchSize            int     `json:"batch_size"`
	MaxConnections       uint    `json:"max_connections"`
	MinJitterMs          int     `json:"min_jitter_ms"`
	MaxJitterMs          int     `json:"max_jitter_ms"`
	SignalCount          int     `json:"signal_count"`
	NumWriters           int     `json:"num_writers"`
	DurationSec          float64 `json:"duration_sec"`
	SignalsPerSec        float64 `json:"signals_per_sec"`
	RequestCount         int64   `json:"request_count"`
	SignalsPerReq        float64 `json:"signals_per_request"`
	AppenderWritesPerSec float64 `json:"appender_writes_per_sec"`
	TotalAppenderWrites  int64   `json:"total_appender_writes"`
	NumCPU               int     `json:"num_cpu"`
	GOMAXPROCS           int     `json:"gomaxprocs"`
	CPUModel             string  `json:"cpu_model"`
	Timestamp            string  `json:"timestamp"`
}

// getCPUInfo gets the CPU model information from /proc/cpuinfo on Linux
func getCPUInfo() string {
	if runtime.GOOS != "linux" {
		return "CPU info only available on Linux"
	}

	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return fmt.Sprintf("Failed to read CPU info: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "model name") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}

	return "CPU model not found"
}

// logSystemInfo logs information about the system running the benchmark
func logSystemInfo(b *testing.B) {
	cpuModel := getCPUInfo()

	b.Logf("System information:")
	b.Logf("  CPU model: %s", cpuModel)
	b.Logf("  CPU count: %d", runtime.NumCPU())
	b.Logf("  GOMAXPROCS: %d", runtime.GOMAXPROCS(0))
	b.Logf("  Go version: %s", runtime.Version())
	b.Logf("  OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)

	// Log memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	b.Logf("  Memory stats:")
	b.Logf("    Alloc: %d MB", memStats.Alloc/1024/1024)
	b.Logf("    Sys: %d MB", memStats.Sys/1024/1024)
	b.Logf("    NumGC: %d", memStats.NumGC)
}

// BenchmarkPrometheusQueueWithJitter benchmarks the throughput of the Prometheus queue implementation
func BenchmarkPrometheusQueueWithJitter(b *testing.B) {
	// Log system information
	logSystemInfo(b)

	// Configure test parameters
	tests := []struct {
		name            string
		batchSize       int
		maxConnections  uint
		minJitterMs     int
		maxJitterMs     int
		signalCount     int // Not used when running with fixed duration
		numWriters      int // Number of concurrent writers
		writerBatchSize int // Batch size for each writer
		diskBatchCount  int
		testDuration    time.Duration // Duration to run each test
	}{
		{
			name:            "500_conn_3000_batch",
			batchSize:       3000,
			maxConnections:  500,
			minJitterMs:     500,
			maxJitterMs:     2500,
			numWriters:      100,
			writerBatchSize: 20000,
			diskBatchCount:  100000,
			testDuration:    30 * time.Second,
		},
		{
			name:            "500_conn_6000_batch",
			batchSize:       6000,
			maxConnections:  500,
			minJitterMs:     500,
			maxJitterMs:     2500,
			numWriters:      100,
			writerBatchSize: 20000,
			diskBatchCount:  100000,
			testDuration:    30 * time.Second,
		},
		{
			name:            "1000_conn_3000_batch",
			batchSize:       3000,
			maxConnections:  1000,
			minJitterMs:     500,
			maxJitterMs:     2500,
			numWriters:      100,
			writerBatchSize: 20000,
			diskBatchCount:  100000,
			testDuration:    30 * time.Second,
		},
	}

	// Setup logger
	logger := log.NewNopLogger()

	// Collect all benchmark results
	var allResults []benchmarkResults

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// Create request counter to track total requests
			requestCounter := &atomic.Int64{}

			// Create test server with jitter
			totalSignals := &atomic.Int64{}
			server := jitterServer(b, tt.minJitterMs, tt.maxJitterMs, requestCounter, totalSignals)

			// Create temporary directory for WAL files
			walDir := createTempDir(b)

			// Create a new Prometheus registry
			registry := prometheusClient.NewRegistry()

			// Create connection config
			connConfig := types.ConnectionConfig{
				URL:              server.URL,
				Timeout:          30 * time.Second,
				BatchCount:       tt.batchSize,
				UserAgent:        "walqueue-benchmark",
				RetryBackoff:     100 * time.Millisecond,
				MaxRetryAttempts: 3,
				FlushInterval:    1 * time.Second,
				Parallelism: types.ParallelismConfig{
					MinConnections:              tt.maxConnections,
					MaxConnections:              tt.maxConnections,
					CheckInterval:               5 * time.Second,
					AllowedDrift:                30 * time.Second,
					Lookback:                    1 * time.Minute,
					ResetInterval:               10 * time.Minute,
					AllowedNetworkErrorFraction: 0.1,
					MinimumScaleDownDrift:       10 * time.Second,
				},
			}

			// Create a prometheus queue
			queue, err := prom_impl.NewQueue(
				"benchmark",
				connConfig,
				walDir,
				uint32(tt.diskBatchCount), // maxSignalsToBatch
				1*time.Second,             // flushInterval
				24*time.Hour,              // ttl (long enough to not drop anything in benchmark)
				registry,
				"benchmark",
				logger,
			)
			require.NoError(b, err)

			// Create context with cancel
			ctx, cancel := context.WithCancel(b.Context())
			defer cancel()

			// Start the queue
			err = queue.Start(ctx)
			require.NoError(b, err)
			defer queue.Stop()

			// Reset benchmark timer to exclude setup time
			b.ResetTimer()

			startTime := time.Now()

			// Create a context with timeout for test duration
			testCtx, testCancel := context.WithTimeout(ctx, tt.testDuration)
			defer testCancel()

			// Run multiple producer goroutines to write signals to the queue using appender
			var wg sync.WaitGroup

			totalAppenderWrites := &atomic.Int64{}
			// Launch multiple writer goroutines
			for i := 0; i < tt.numWriters; i++ {
				wg.Add(1)
				go func(writerID int) {
					defer wg.Done()

					// Track signals sent by this writer
					writerSignals := 0

					// Generate and append metrics
					baseTimestamp := time.Now().UnixNano() / int64(time.Millisecond)

					// Use configured batch size for appending
					batchSize := tt.writerBatchSize
					j := 0

					// Run until test duration completes
					for {
						select {
						case <-testCtx.Done():
							b.Logf("Writer %d finished: appended %d signals", writerID, writerSignals)
							return
						default:
							// Get an appender from the queue
							appender := queue.Appender(testCtx)
							// Append metrics in batches
							for k := 0; k < batchSize; k++ {
								// Create labels for the metric
								lbls := labels.FromStrings(
									"__name__", fmt.Sprintf("test_metric_%d", rand.Uint32()),
									"instance", fmt.Sprintf("instance_%d", writerID),
									"job", "benchmark",
								)

								// Append the metric
								timestamp := baseTimestamp + int64(j+k)
								value := float64(writerID*10000 + j + k)
								_, err := appender.Append(storage.SeriesRef(0), lbls, timestamp, value)
								if err != nil {
									b.Logf("Error appending metric: %v", err)
									return
								}
							}
							totalAppenderWrites.Add(int64(batchSize))

							// Commit the batch
							if err := appender.Commit(); err != nil {
								b.Logf("Error committing batch: %v", err)
								return
							}

							// Update signals counter
							writerSignals += batchSize
							j += batchSize

							// Add a small sleep to simulate real-world writing patterns and reduce contention
							if tt.numWriters > 10 {
								// Random sleep between 1-5ms to prevent all writers from hitting the queue at once
								time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
							}
						}
					}
				}(i)
			}

			// Wait until we've received enough requests or hit timeout
			monitorDone := make(chan struct{})
			go func() {
				defer close(monitorDone)

				ticker := time.NewTicker(1 * time.Second)
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						totalSent := totalSignals.Load()

						// Log progress
						if totalSent > 0 {
							b.Logf("Progress: ~%d signals sent, %d requests made",
								totalSent, requestCounter.Load())
						}

					case <-testCtx.Done():
						b.Logf("Test duration completed")
						return
					case <-ctx.Done():
						return
					}
				}
			}()

			// Wait for the test duration to complete
			<-testCtx.Done()

			// Wait for all writers to complete
			wg.Wait()

			// Wait for the monitor to finish
			<-monitorDone

			// Add a little extra time to let any in-flight requests finish
			time.Sleep(5 * time.Second)

			// Calculate and report results
			duration := time.Since(startTime)
			durationSec := duration.Seconds()
			throughput := float64(totalSignals.Load()) / durationSec
			reqCount := requestCounter.Load()
			signalsPerReq := float64(0)
			if reqCount > 0 {
				signalsPerReq = float64(totalSignals.Load()) / float64(reqCount)
			}

			// Report metrics for Go benchmarking tool
			b.ReportMetric(throughput, "signals/sec")
			b.ReportMetric(float64(reqCount), "requests")
			b.ReportMetric(signalsPerReq, "signals/request")

			// Log summary
			b.Logf("Benchmark completed: processed ~%d signals in %v (%.2f signals/sec), %d requests, %.2f signals/request, %.2f appender writes/sec, %d total appender writes",
				totalSignals.Load(), duration, throughput, reqCount, signalsPerReq, float64(totalAppenderWrites.Load())/durationSec, totalAppenderWrites.Load())

			// Create a result record
			result := benchmarkResults{
				TestName:             tt.name,
				BatchSize:            tt.batchSize,
				MaxConnections:       tt.maxConnections,
				MinJitterMs:          tt.minJitterMs,
				MaxJitterMs:          tt.maxJitterMs,
				SignalCount:          int(totalSignals.Load()), // Record actual signals sent
				NumWriters:           tt.numWriters,
				DurationSec:          durationSec,
				SignalsPerSec:        throughput,
				RequestCount:         reqCount,
				SignalsPerReq:        signalsPerReq,
				AppenderWritesPerSec: float64(totalAppenderWrites.Load()) / durationSec,
				TotalAppenderWrites:  totalAppenderWrites.Load(),
				NumCPU:               runtime.NumCPU(),
				GOMAXPROCS:           runtime.GOMAXPROCS(0),
				CPUModel:             getCPUInfo(),
				Timestamp:            time.Now().Format(time.RFC3339),
			}

			// Add to results collection
			allResults = append(allResults, result)
		})
	}

	// Output JSON results at the end of all benchmarks
	if resultJSON, err := json.MarshalIndent(allResults, "", "  "); err == nil {
		b.Logf("Benchmark results summary (JSON):\n%s", string(resultJSON))

		// Create dedicated results directory if it doesn't exist
		resultsDir := filepath.Join("benchmark_results")
		if err := os.MkdirAll(resultsDir, 0755); err != nil {
			b.Logf("Warning: couldn't create benchmark_results directory: %v", err)
			resultsDir = os.TempDir() // Fall back to temp dir
		}

		// Add optimization info and timestamp to filename for tracking changes over time
		timestamp := time.Now().Format("20060102_150405")
		benchType := "prometheus"

		resultsFile := filepath.Join(
			resultsDir,
			fmt.Sprintf("%s_%s.json", benchType, timestamp),
		)

		// Add Git commit hash if available
		gitHashCmd := exec.Command("git", "rev-parse", "--short", "HEAD")
		gitHashCmd.Dir = "."
		if gitHash, err := gitHashCmd.Output(); err == nil {
			// Add extra metadata to the results
			type BenchmarkResultsWithMeta struct {
				Results  []benchmarkResults `json:"results"`
				Metadata struct {
					GitCommit           string    `json:"git_commit"`
					Timestamp           time.Time `json:"timestamp"`
					Optimizations       string    `json:"optimizations"`
					GoVersion           string    `json:"go_version"`
					NumCPU              int       `json:"num_cpu"`
					GOMAXPROCS          int       `json:"gomaxprocs"`
					CPUModel            string    `json:"cpu_model"`
					TotalSignals        int64     `json:"total_signals"`
					TestDurationSec     float64   `json:"test_duration_sec"`
					TotalAppenderWrites int64     `json:"total_appender_writes"`
				} `json:"metadata"`
			}

			metaResults := BenchmarkResultsWithMeta{
				Results: allResults,
			}
			metaResults.Metadata.GitCommit = strings.TrimSpace(string(gitHash))
			metaResults.Metadata.Timestamp = time.Now()
			metaResults.Metadata.Optimizations = os.Getenv("TEST")
			metaResults.Metadata.GoVersion = runtime.Version()
			metaResults.Metadata.NumCPU = runtime.NumCPU()
			metaResults.Metadata.GOMAXPROCS = runtime.GOMAXPROCS(0)
			metaResults.Metadata.CPUModel = getCPUInfo()

			// Calculate total signals across all test runs
			var totalSignals int64
			var totalDuration float64
			var totalAppenderWrites int64
			for _, res := range allResults {
				totalSignals += int64(res.SignalCount)
				totalDuration += res.DurationSec
				totalAppenderWrites += res.TotalAppenderWrites
			}

			// Add total signal count and appender writes to metadata
			metaResults.Metadata.TotalSignals = totalSignals
			metaResults.Metadata.TestDurationSec = totalDuration
			metaResults.Metadata.TotalAppenderWrites = totalAppenderWrites

			resultJSON, err = json.MarshalIndent(metaResults, "", "  ")
			if err != nil {
				b.Logf("Warning: couldn't add metadata to results: %v", err)
			}
		}

		// Write the benchmark results to disk for future comparison
		if err := os.WriteFile(resultsFile, resultJSON, 0644); err == nil {
			b.Logf("Benchmark results saved to: %s", resultsFile)
		} else {
			b.Logf("Failed to save benchmark results: %v", err)
		}
	} else {
		b.Logf("Failed to serialize benchmark results: %v", err)
	}
}
