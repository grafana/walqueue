package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

// TestCompressionBenchmark benchmarks compression performance and ratio between
// different compression algorithms using realistic node_exporter metrics
func TestCompressionBenchmark(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	// Configure test parameters
	const (
		metricsPerCommit = 100000  // Number of metrics per append commit
		totalMetrics     = 1000000 // Total number of metrics to generate
		commitCount      = totalMetrics / metricsPerCommit
		benchmarkRuns    = 5 // Number of times to run decompression benchmarks for more accurate timing
	)

	// Set up metrics with random seed for reproducibility
	rand.Seed(12345)

	// Generate node_exporter-like metrics
	metrics := generateNodeExporterMetrics(totalMetrics)
	require.Equal(t, totalMetrics, len(metrics), "Expected to generate exactly %d metrics", totalMetrics)

	t.Logf("Generated %d metrics in total, will commit in %d batches of %d metrics each",
		totalMetrics, commitCount, metricsPerCommit)

	// Benchmark each compression algorithm
	// 1. Snappy compression
	t.Run("Snappy", func(t *testing.T) {
		var totalUncompressedSize, totalCompressedSize int64
		compressionTimeNs := int64(0)

		for i := 0; i < commitCount; i++ {
			start := i * metricsPerCommit
			end := start + metricsPerCommit
			if end > totalMetrics {
				end = totalMetrics
			}

			// Create WriteRequest with metrics batch
			writeReq := &prompb.WriteRequest{
				Timeseries: metrics[start:end],
			}

			// Marshal to protobuf
			data, err := writeReq.Marshal()
			require.NoError(t, err)
			uncompressedSize := int64(len(data))
			totalUncompressedSize += uncompressedSize

			// Compress with Snappy
			compressStart := time.Now()
			compressed := snappy.Encode(nil, data)
			compressionTime := time.Since(compressStart)
			compressionTimeNs += compressionTime.Nanoseconds()

			compressedSize := int64(len(compressed))
			totalCompressedSize += compressedSize

			// Verify decompression works
			_, err = snappy.Decode(nil, compressed)
			require.NoError(t, err)

			t.Logf("Batch %d: Snappy - Uncompressed: %d bytes, Compressed: %d bytes, Ratio: %.2f%%, Time: %v",
				i+1, uncompressedSize, compressedSize,
				float64(compressedSize)/float64(uncompressedSize)*100,
				compressionTime)
		}

		compressionRatio := float64(totalCompressedSize) / float64(totalUncompressedSize) * 100
		avgCompressionTime := time.Duration(compressionTimeNs / int64(commitCount))

		t.Logf("Snappy Summary - Total Uncompressed: %d bytes, Total Compressed: %d bytes",
			totalUncompressedSize, totalCompressedSize)
		t.Logf("Snappy Summary - Compression Ratio: %.2f%%, Avg Time per batch: %v",
			compressionRatio, avgCompressionTime)
	})

	// 1b. Gzip compression
	t.Run("Gzip", func(t *testing.T) {
		var totalUncompressedSize, totalCompressedSize int64
		compressionTimeNs := int64(0)

		for i := 0; i < commitCount; i++ {
			start := i * metricsPerCommit
			end := start + metricsPerCommit
			if end > totalMetrics {
				end = totalMetrics
			}

			// Create WriteRequest with metrics batch
			writeReq := &prompb.WriteRequest{
				Timeseries: metrics[start:end],
			}

			// Marshal to protobuf
			data, err := writeReq.Marshal()
			require.NoError(t, err)
			uncompressedSize := int64(len(data))
			totalUncompressedSize += uncompressedSize

			// Compress with Gzip
			compressStart := time.Now()
			var b bytes.Buffer
			gzipWriter, err := gzip.NewWriterLevel(&b, gzip.BestSpeed)
			require.NoError(t, err)
			_, err = gzipWriter.Write(data)
			require.NoError(t, err)
			err = gzipWriter.Close()
			require.NoError(t, err)
			compressed := b.Bytes()
			compressionTime := time.Since(compressStart)
			compressionTimeNs += compressionTime.Nanoseconds()

			compressedSize := int64(len(compressed))
			totalCompressedSize += compressedSize

			// Verify decompression works
			gzipReader, err := gzip.NewReader(bytes.NewReader(compressed))
			require.NoError(t, err)
			_, err = io.ReadAll(gzipReader)
			require.NoError(t, err)
			err = gzipReader.Close()
			require.NoError(t, err)

			t.Logf("Batch %d: Gzip - Uncompressed: %d bytes, Compressed: %d bytes, Ratio: %.2f%%, Time: %v",
				i+1, uncompressedSize, compressedSize,
				float64(compressedSize)/float64(uncompressedSize)*100,
				compressionTime)
		}

		compressionRatio := float64(totalCompressedSize) / float64(totalUncompressedSize) * 100
		avgCompressionTime := time.Duration(compressionTimeNs / int64(commitCount))

		t.Logf("Gzip Summary - Total Uncompressed: %d bytes, Total Compressed: %d bytes",
			totalUncompressedSize, totalCompressedSize)
		t.Logf("Gzip Summary - Compression Ratio: %.2f%%, Avg Time per batch: %v",
			compressionRatio, avgCompressionTime)
	})

	// 2. S2 compression
	t.Run("S2", func(t *testing.T) {
		var totalUncompressedSize, totalCompressedSize int64
		compressionTimeNs := int64(0)

		for i := 0; i < commitCount; i++ {
			start := i * metricsPerCommit
			end := start + metricsPerCommit
			if end > totalMetrics {
				end = totalMetrics
			}

			// Create WriteRequest with metrics batch
			writeReq := &prompb.WriteRequest{
				Timeseries: metrics[start:end],
			}

			// Marshal to protobuf
			data, err := writeReq.Marshal()
			require.NoError(t, err)
			uncompressedSize := int64(len(data))
			totalUncompressedSize += uncompressedSize

			// Compress with S2
			compressStart := time.Now()
			compressed := s2.Encode(nil, data)
			compressionTime := time.Since(compressStart)
			compressionTimeNs += compressionTime.Nanoseconds()

			compressedSize := int64(len(compressed))
			totalCompressedSize += compressedSize

			// Verify decompression works
			_, err = s2.Decode(nil, compressed)
			require.NoError(t, err)

			t.Logf("Batch %d: S2 - Uncompressed: %d bytes, Compressed: %d bytes, Ratio: %.2f%%, Time: %v",
				i+1, uncompressedSize, compressedSize,
				float64(compressedSize)/float64(uncompressedSize)*100,
				compressionTime)
		}

		compressionRatio := float64(totalCompressedSize) / float64(totalUncompressedSize) * 100
		avgCompressionTime := time.Duration(compressionTimeNs / int64(commitCount))

		t.Logf("S2 Summary - Total Uncompressed: %d bytes, Total Compressed: %d bytes",
			totalUncompressedSize, totalCompressedSize)
		t.Logf("S2 Summary - Compression Ratio: %.2f%%, Avg Time per batch: %v",
			compressionRatio, avgCompressionTime)
	})

	// 3. Zstd compression with different compression levels
	zstdLevels := []struct {
		name  string
		level zstd.EncoderLevel
	}{
		{"Zstd-Fastest", zstd.SpeedFastest},          // Fastest compression
		{"Zstd-Default", zstd.SpeedDefault},          // Default balance of speed/compression
		{"Zstd-Better", zstd.SpeedBetterCompression}, // Better compression, slower
	}

	for _, levelConfig := range zstdLevels {
		t.Run(levelConfig.name, func(t *testing.T) {
			// Initialize zstd encoder with the current level
			zstdEncoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(levelConfig.level))
			require.NoError(t, err)

			var totalUncompressedSize, totalCompressedSize int64
			compressionTimeNs := int64(0)

			// Create zstd decoder for verification
			zstdDecoder, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer zstdDecoder.Close()

			for i := 0; i < commitCount; i++ {
				start := i * metricsPerCommit
				end := start + metricsPerCommit
				if end > totalMetrics {
					end = totalMetrics
				}

				// Create WriteRequest with metrics batch
				writeReq := &prompb.WriteRequest{
					Timeseries: metrics[start:end],
				}

				// Marshal to protobuf
				data, err := writeReq.Marshal()
				require.NoError(t, err)
				uncompressedSize := int64(len(data))
				totalUncompressedSize += uncompressedSize

				// Compress with Zstd
				compressStart := time.Now()
				compressed := zstdEncoder.EncodeAll(data, nil)
				compressionTime := time.Since(compressStart)
				compressionTimeNs += compressionTime.Nanoseconds()

				compressedSize := int64(len(compressed))
				totalCompressedSize += compressedSize

				// Verify decompression works
				_, err = zstdDecoder.DecodeAll(compressed, nil)
				require.NoError(t, err)

				t.Logf("Batch %d: %s - Uncompressed: %d bytes, Compressed: %d bytes, Ratio: %.2f%%, Time: %v",
					i+1, levelConfig.name, uncompressedSize, compressedSize,
					float64(compressedSize)/float64(uncompressedSize)*100,
					compressionTime)
			}

			compressionRatio := float64(totalCompressedSize) / float64(totalUncompressedSize) * 100
			avgCompressionTime := time.Duration(compressionTimeNs / int64(commitCount))

			t.Logf("%s Summary - Total Uncompressed: %d bytes, Total Compressed: %d bytes",
				levelConfig.name, totalUncompressedSize, totalCompressedSize)
			t.Logf("%s Summary - Compression Ratio: %.2f%%, Avg Time per batch: %v",
				levelConfig.name, compressionRatio, avgCompressionTime)
		})
	}

	// Benchmark memory usage during compression
	t.Run("MemoryUsageBenchmark", func(t *testing.T) {
		// Generate a larger batch for clearer memory usage measurement
		sampleBatch := generateNodeExporterMetrics(metricsPerCommit * 2)

		// Create WriteRequest with metrics batch
		writeReq := &prompb.WriteRequest{
			Timeseries: sampleBatch,
		}

		// Marshal to protobuf
		data, err := writeReq.Marshal()
		require.NoError(t, err)
		uncompressedSize := len(data)

		// Measure memory before and after compression
		var m1, m2 runtime.MemStats

		// 1. Snappy
		runtime.GC() // Force GC to get more accurate measurements
		runtime.ReadMemStats(&m1)
		snappyCompressed := snappy.Encode(nil, data)
		runtime.ReadMemStats(&m2)
		snappyMemUsage := m2.HeapAlloc - m1.HeapAlloc

		// 1b. Gzip
		runtime.GC()
		runtime.ReadMemStats(&m1)
		var gzipBuf bytes.Buffer
		gzipWriter, _ := gzip.NewWriterLevel(&gzipBuf, gzip.BestSpeed)
		_, _ = gzipWriter.Write(data)
		_ = gzipWriter.Close()
		gzipCompressed := gzipBuf.Bytes()
		runtime.ReadMemStats(&m2)
		gzipMemUsage := m2.HeapAlloc - m1.HeapAlloc

		// 2. S2
		runtime.GC()
		runtime.ReadMemStats(&m1)
		s2Compressed := s2.Encode(nil, data)
		runtime.ReadMemStats(&m2)
		s2MemUsage := m2.HeapAlloc - m1.HeapAlloc

		// 3. Zstd-Fastest
		runtime.GC()
		zstdFastestEncoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		runtime.ReadMemStats(&m1)
		zstdFastestCompressed := zstdFastestEncoder.EncodeAll(data, nil)
		runtime.ReadMemStats(&m2)
		zstdFastestMemUsage := m2.HeapAlloc - m1.HeapAlloc

		// 4. Zstd-Better
		runtime.GC()
		zstdBetterEncoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
		runtime.ReadMemStats(&m1)
		zstdBetterCompressed := zstdBetterEncoder.EncodeAll(data, nil)
		runtime.ReadMemStats(&m2)
		zstdBetterMemUsage := m2.HeapAlloc - m1.HeapAlloc

		// Report memory usage results
		t.Logf("\nMemory Usage During Compression:")
		t.Logf("-------------------------------------")
		t.Logf("Format       | Memory Used | Output Size | Ratio")
		t.Logf("-------------------------------------")
		t.Logf("Uncompressed | %11d | %11d | 100.00%%", 0, uncompressedSize)
		t.Logf("Snappy       | %11d | %11d | %.2f%%", snappyMemUsage, len(snappyCompressed),
			float64(len(snappyCompressed))/float64(uncompressedSize)*100)
		t.Logf("Gzip         | %11d | %11d | %.2f%%", gzipMemUsage, len(gzipCompressed),
			float64(len(gzipCompressed))/float64(uncompressedSize)*100)
		t.Logf("S2           | %11d | %11d | %.2f%%", s2MemUsage, len(s2Compressed),
			float64(len(s2Compressed))/float64(uncompressedSize)*100)
		t.Logf("Zstd-Fastest | %11d | %11d | %.2f%%", zstdFastestMemUsage, len(zstdFastestCompressed),
			float64(len(zstdFastestCompressed))/float64(uncompressedSize)*100)
		t.Logf("Zstd-Better  | %11d | %11d | %.2f%%", zstdBetterMemUsage, len(zstdBetterCompressed),
			float64(len(zstdBetterCompressed))/float64(uncompressedSize)*100)
	})

	// Now specifically benchmark decompression performance on the same data
	t.Run("DecompressionBenchmark", func(t *testing.T) {
		// Generate a sample batch for benchmarking
		sampleBatch := generateNodeExporterMetrics(metricsPerCommit)

		// Create WriteRequest with metrics batch
		writeReq := &prompb.WriteRequest{
			Timeseries: sampleBatch,
		}

		// Marshal to protobuf
		data, err := writeReq.Marshal()
		require.NoError(t, err)
		uncompressedSize := len(data)

		// Prepare compressed versions
		compressedData := make(map[string][]byte)
		compressedSizes := make(map[string]int)

		// 1. Snappy
		compressedData["Snappy"] = snappy.Encode(nil, data)
		compressedSizes["Snappy"] = len(compressedData["Snappy"])

		// 1b. Gzip
		var gzipBuf bytes.Buffer
		gzipWriter, _ := gzip.NewWriterLevel(&gzipBuf, gzip.BestSpeed)
		_, _ = gzipWriter.Write(data)
		_ = gzipWriter.Close()
		compressedData["Gzip"] = gzipBuf.Bytes()
		compressedSizes["Gzip"] = len(compressedData["Gzip"])

		// 2. S2
		compressedData["S2"] = s2.Encode(nil, data)
		compressedSizes["S2"] = len(compressedData["S2"])

		// 3. Zstd with various levels
		// Initialize zstd encoders for each level
		zstdFastestEncoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		zstdDefaultEncoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		zstdBetterEncoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))

		// Compress with each encoder
		compressedData["Zstd-Fastest"] = zstdFastestEncoder.EncodeAll(data, nil)
		compressedData["Zstd-Default"] = zstdDefaultEncoder.EncodeAll(data, nil)
		compressedData["Zstd-Better"] = zstdBetterEncoder.EncodeAll(data, nil)

		compressedSizes["Zstd-Fastest"] = len(compressedData["Zstd-Fastest"])
		compressedSizes["Zstd-Default"] = len(compressedData["Zstd-Default"])
		compressedSizes["Zstd-Better"] = len(compressedData["Zstd-Better"])

		// Create zstd decoder (reused for all zstd levels)
		zstdDecoder, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer zstdDecoder.Close()

		// Run decompression benchmarks
		const benchmarkRuns = 100 // More runs for more accurate measurement
		decompressTimes := make(map[string]int64)

		// Benchmark Snappy decompression
		decompressTimes["Snappy"] = 0
		for i := 0; i < benchmarkRuns; i++ {
			start := time.Now()
			_, err := snappy.Decode(nil, compressedData["Snappy"])
			require.NoError(t, err)
			decompressTimes["Snappy"] += time.Since(start).Nanoseconds()
		}

		// Benchmark Gzip decompression
		decompressTimes["Gzip"] = 0
		for i := 0; i < benchmarkRuns; i++ {
			start := time.Now()
			gzipReader, err := gzip.NewReader(bytes.NewReader(compressedData["Gzip"]))
			require.NoError(t, err)
			_, err = io.ReadAll(gzipReader)
			require.NoError(t, err)
			err = gzipReader.Close()
			require.NoError(t, err)
			decompressTimes["Gzip"] += time.Since(start).Nanoseconds()
		}

		// Benchmark S2 decompression
		decompressTimes["S2"] = 0
		for i := 0; i < benchmarkRuns; i++ {
			start := time.Now()
			_, err := s2.Decode(nil, compressedData["S2"])
			require.NoError(t, err)
			decompressTimes["S2"] += time.Since(start).Nanoseconds()
		}

		// Benchmark Zstd decompression for each level
		for _, level := range []string{"Zstd-Fastest", "Zstd-Default", "Zstd-Better"} {
			decompressTimes[level] = 0
			for i := 0; i < benchmarkRuns; i++ {
				start := time.Now()
				_, err := zstdDecoder.DecodeAll(compressedData[level], nil)
				require.NoError(t, err)
				decompressTimes[level] += time.Since(start).Nanoseconds()
			}
		}

		// Report results
		t.Logf("\nDecompression Benchmark Results (avg of %d runs):", benchmarkRuns)
		t.Logf("---------------------------------------------------------")
		t.Logf("Format       | Compressed Size | Ratio | Decompress Time")
		t.Logf("---------------------------------------------------------")

		// Calculate average times and print results
		avgTimes := make(map[string]time.Duration)
		for name, totalTime := range decompressTimes {
			avgTimes[name] = time.Duration(totalTime / benchmarkRuns)
			ratio := float64(compressedSizes[name]) / float64(uncompressedSize) * 100
			t.Logf("%-12s | %15d | %5.2f%% | %14v",
				name, compressedSizes[name], ratio, avgTimes[name])
		}

		// Compare ratios and performance
		t.Logf("\nRelative Performance Comparison:")

		// Size comparison baseline (using Snappy)
		snappySize := compressedSizes["Snappy"]
		for name, size := range compressedSizes {
			if name == "Snappy" {
				continue
			}
			sizeImprovement := (1.0 - float64(size)/float64(snappySize)) * 100
			if sizeImprovement > 0 {
				t.Logf("%s size vs Snappy: %+.2f%% (%s is smaller by this percentage)",
					name, sizeImprovement, name)
			} else {
				t.Logf("%s size vs Snappy: %+.2f%% (%s is larger by this percentage)",
					name, sizeImprovement, name)
			}
		}

		// Speed comparison baseline (using Snappy)
		snappyTime := avgTimes["Snappy"]
		for name, avgTime := range avgTimes {
			if name == "Snappy" {
				continue
			}
			speedRatio := float64(avgTime.Nanoseconds()) / float64(snappyTime.Nanoseconds())

			if speedRatio < 1.0 {
				t.Logf("%s speed vs Snappy: %.2fx faster", name, 1/speedRatio)
			} else {
				t.Logf("%s speed vs Snappy: %.2fx slower", name, speedRatio)
			}
		}
	})
}

// generateNodeExporterMetrics generates a set of prompb.TimeSeries that mimic node_exporter metrics
func generateNodeExporterMetrics(count int) []prompb.TimeSeries {
	// Common node_exporter metric names
	metricNames := []string{
		"node_cpu_seconds_total",
		"node_memory_MemFree_bytes",
		"node_memory_MemTotal_bytes",
		"node_disk_io_time_seconds_total",
		"node_disk_read_bytes_total",
		"node_disk_write_bytes_total",
		"node_disk_reads_completed_total",
		"node_disk_writes_completed_total",
		"node_network_receive_bytes_total",
		"node_network_transmit_bytes_total",
		"node_network_receive_packets_total",
		"node_network_transmit_packets_total",
		"node_filesystem_avail_bytes",
		"node_filesystem_size_bytes",
		"node_load1",
		"node_load5",
		"node_load15",
		"process_cpu_seconds_total",
		"go_gc_duration_seconds",
		"go_goroutines",
	}

	// Common node_exporter label names and potential values
	instanceValues := []string{"prod-01", "prod-02", "prod-03", "stage-01", "dev-01"}
	jobValues := []string{"node", "prometheus", "alertmanager", "grafana"}
	cpuValues := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}
	modeValues := []string{"idle", "user", "system", "iowait", "irq", "softirq", "steal", "nice"}
	diskValues := []string{"sda", "sdb", "sdc", "nvme0n1", "nvme1n1"}
	fsTypeValues := []string{"ext4", "xfs", "btrfs", "tmpfs"}
	mountpointValues := []string{"/", "/home", "/var", "/tmp", "/boot"}
	interfaceValues := []string{"eth0", "eth1", "lo", "bond0", "docker0"}

	// Create time range for the metrics (last hour with 15s intervals)
	now := time.Now().UnixMilli()
	startTime := now - 3600*1000 // 1 hour ago

	// Generate the metrics
	result := make([]prompb.TimeSeries, 0, count)
	for i := 0; i < count; i++ {
		// Select metric type and create appropriate labels
		metricIdx := rand.Intn(len(metricNames))
		metricName := metricNames[metricIdx]

		// Base labels that all metrics have
		labelSet := []labels.Label{
			{Name: "__name__", Value: metricName},
			{Name: "instance", Value: instanceValues[rand.Intn(len(instanceValues))]},
			{Name: "job", Value: jobValues[rand.Intn(len(jobValues))]},
		}

		// Add specific labels based on metric type
		switch {
		case metricName == "node_cpu_seconds_total":
			labelSet = append(labelSet,
				labels.Label{Name: "cpu", Value: cpuValues[rand.Intn(len(cpuValues))]},
				labels.Label{Name: "mode", Value: modeValues[rand.Intn(len(modeValues))]},
			)
		case metricName == "node_disk_io_time_seconds_total" ||
			metricName == "node_disk_read_bytes_total" ||
			metricName == "node_disk_write_bytes_total" ||
			metricName == "node_disk_reads_completed_total" ||
			metricName == "node_disk_writes_completed_total":
			labelSet = append(labelSet,
				labels.Label{Name: "device", Value: diskValues[rand.Intn(len(diskValues))]},
			)
		case metricName == "node_filesystem_avail_bytes" ||
			metricName == "node_filesystem_size_bytes":
			labelSet = append(labelSet,
				labels.Label{Name: "device", Value: diskValues[rand.Intn(len(diskValues))]},
				labels.Label{Name: "fstype", Value: fsTypeValues[rand.Intn(len(fsTypeValues))]},
				labels.Label{Name: "mountpoint", Value: mountpointValues[rand.Intn(len(mountpointValues))]},
			)
		case metricName == "node_network_receive_bytes_total" ||
			metricName == "node_network_transmit_bytes_total" ||
			metricName == "node_network_receive_packets_total" ||
			metricName == "node_network_transmit_packets_total":
			labelSet = append(labelSet,
				labels.Label{Name: "device", Value: interfaceValues[rand.Intn(len(interfaceValues))]},
			)
		}

		// Convert labels.Label to prompb.Label
		promLabels := make([]prompb.Label, 0, len(labelSet))
		for _, l := range labelSet {
			promLabels = append(promLabels, prompb.Label{
				Name:  l.Name,
				Value: l.Value,
			})
		}

		// Generate a single sample for this series
		// For counter metrics, generate values that increase over time
		var value float64
		timestamp := startTime + int64(rand.Intn(3600))*1000 // Random time within the last hour

		if strings.Contains(metricName, "total") {
			// For counters, use larger values that would realistically accumulate
			value = float64(rand.Intn(1000000) + 10000)
		} else if strings.Contains(metricName, "bytes") {
			// For byte metrics, use larger values
			value = float64(rand.Intn(100000000) + 1000000)
		} else if strings.Contains(metricName, "seconds") {
			// For time metrics, use smaller values
			value = rand.Float64() * 100
		} else if strings.HasPrefix(metricName, "node_load") {
			// For load averages, use realistic values between 0.01 and 10
			value = rand.Float64() * 10
		} else {
			// For other metrics, use general purpose random values
			value = rand.Float64() * 1000
		}

		// Create the time series
		ts := prompb.TimeSeries{
			Labels: promLabels,
			Samples: []prompb.Sample{
				{
					Value:     value,
					Timestamp: timestamp,
				},
			},
		}

		result = append(result, ts)
	}

	return result
}
