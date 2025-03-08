package serialization

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	v2 "github.com/grafana/walqueue/types/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

// BenchmarkCompression tests the performance of different compression algorithms with node_exporter-like metrics
func BenchmarkCompression(b *testing.B) {
	/*
		Run with:
		go test -bench="BenchmarkCompression" -benchmem -benchtime "5s" ./serialization
	*/
	logger := log.NewNopLogger()
	
	// Create realistic node_exporter metrics
	metrics := createNodeExporterMetrics()
	b.Logf("Generated %d node_exporter-like metrics", len(metrics))
	
	// Create serializers for each compression type
	serializers := []struct {
		name        string
		compression CompressionType
	}{
		{name: "snappy", compression: CompressionSnappy},
		{name: "zstd", compression: CompressionZstd},
	}

	for _, tc := range serializers {
		b.Run(tc.name, func(b *testing.B) {
			// Create a serializer with the specific compression type
			s := &serializer{
				ser:         v2.NewFormat(),
				compression: tc.compression,
				logger:      logger,
			}
			
			// Initialize zstd encoder if needed
			if tc.compression == CompressionZstd {
				var err error
				s.zstdEncoder, err = zstd.NewWriter(nil)
				require.NoError(b, err)
				defer s.zstdEncoder.Close()
			}
			
			// Add metrics to the serializer
			for _, metric := range metrics {
				err := s.ser.AddPrometheusMetric(
					metric.ts,
					metric.value,
					metric.labels,
					nil, nil, nil,
				)
				require.NoError(b, err)
			}
			
			// Run a single marshal first to get the sizes
			var uncompressedSize, compressedSize int
			
			err := s.ser.Marshal(func(meta map[string]string, buf []byte) error {
				uncompressedSize = len(buf)
				
				var compressed []byte
				if tc.compression == CompressionZstd {
					compressed = s.zstdEncoder.EncodeAll(buf, nil)
				} else {
					compressed = snappy.Encode(nil, buf)
				}
				
				compressedSize = len(compressed)
				return nil
			})
			require.NoError(b, err)
			
			// Report metrics
			b.ReportMetric(float64(uncompressedSize)/1024, "uncompressed_KB")
			b.ReportMetric(float64(compressedSize)/1024, "compressed_KB")
			
			// Report compression ratio
			if uncompressedSize > 0 && compressedSize > 0 {
				ratio := float64(uncompressedSize) / float64(compressedSize)
				b.ReportMetric(ratio, "compression_ratio")
				
				// Print additional information for clarity
				b.Logf("%s: Uncompressed: %.2f KB, Compressed: %.2f KB, Ratio: %.2f:1", 
					tc.name, 
					float64(uncompressedSize)/1024, 
					float64(compressedSize)/1024, 
					ratio)
			}
			
			// Now run the actual benchmark
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := s.ser.Marshal(func(meta map[string]string, buf []byte) error {
					if tc.compression == CompressionZstd {
						_ = s.zstdEncoder.EncodeAll(buf, nil)
					} else {
						_ = snappy.Encode(nil, buf)
					}
					return nil
				})
				require.NoError(b, err)
			}
		})
	}
}

// nodeExporterMetric represents a single metric from node_exporter
type nodeExporterMetric struct {
	ts     int64
	value  float64
	labels labels.Labels
}

// createNodeExporterMetrics creates a realistic set of node_exporter metrics
func createNodeExporterMetrics() []nodeExporterMetric {
	metrics := []nodeExporterMetric{}
	now := time.Now().UnixMilli()
	
	// Common label names used by node_exporter
	commonLabels := []string{
		"cpu", "device", "fstype", "instance", "job", "mode", 
		"mountpoint", "state", "uuid", "hostname", "service",
		"path", "interface", "name", "le", "boottime", "disk",
	}
	
	// Common metric prefixes used by node_exporter
	metricPrefixes := []string{
		"node_cpu_seconds_total", "node_disk_io_time_seconds_total",
		"node_filesystem_avail_bytes", "node_filesystem_size_bytes",
		"node_load1", "node_load5", "node_load15",
		"node_memory_MemAvailable_bytes", "node_memory_MemFree_bytes",
		"node_memory_MemTotal_bytes", "node_memory_SwapFree_bytes",
		"node_network_receive_bytes_total", "node_network_transmit_bytes_total",
		"node_time_seconds", "node_uname_info", "node_filefd_allocated",
		"node_disk_read_bytes_total", "node_disk_write_bytes_total",
		"node_filesystem_files", "node_filesystem_files_free",
		"process_cpu_seconds_total", "process_resident_memory_bytes",
		"process_virtual_memory_bytes", "process_open_fds",
		"process_max_fds", "process_start_time_seconds",
	}
	
	// Generate about 5000 metrics, similar to a large node_exporter scrape
	for i := 0; i < 5000; i++ {
		metricName := metricPrefixes[i%len(metricPrefixes)]
		labelCount := 3 + (i % 5) // Vary between 3-7 labels
		
		// Create labels for this metric
		metricLabels := labels.Labels{}
		
		// Always add instance and job labels
		metricLabels = append(metricLabels, labels.Label{
			Name:  "instance", 
			Value: "localhost:9100",
		})
		metricLabels = append(metricLabels, labels.Label{
			Name:  "job", 
			Value: "node_exporter",
		})
		
		// Add the metric name as a label
		metricLabels = append(metricLabels, labels.Label{
			Name:  "__name__", 
			Value: metricName,
		})
		
		// Add additional labels based on the metric type
		for j := 0; j < labelCount; j++ {
			labelName := commonLabels[j%len(commonLabels)]
			
			// Skip if this label would be a duplicate
			duplicate := false
			for _, l := range metricLabels {
				if l.Name == labelName {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			
			// Generate appropriate label values based on the label name
			var labelValue string
			switch labelName {
			case "cpu":
				labelValue = fmt.Sprintf("%d", i%8)
			case "device":
				labelValue = fmt.Sprintf("sd%c", 'a'+(i%3))
			case "fstype":
				labelValue = []string{"ext4", "xfs", "btrfs"}[i%3]
			case "mode":
				labelValue = []string{"user", "system", "iowait", "idle", "irq"}[i%5]
			case "mountpoint":
				labelValue = []string{"/", "/home", "/var", "/tmp", "/boot"}[i%5]
			case "state":
				labelValue = []string{"running", "waiting", "stopped"}[i%3]
			case "interface":
				labelValue = []string{"eth0", "lo", "wlan0", "docker0"}[i%4]
			default:
				// Random string for other labels
				labelValue = fmt.Sprintf("value%d", i%10)
			}
			
			metricLabels = append(metricLabels, labels.Label{
				Name:  labelName,
				Value: labelValue,
			})
		}
		
		// Generate appropriate value based on the metric type
		var value float64
		if metricName == "node_load1" || metricName == "node_load5" || metricName == "node_load15" {
			value = 1.0 + (rand.Float64() * 2.0)
		} else if metricName == "node_memory_MemTotal_bytes" {
			value = 16000000000.0 // 16GB
		} else if metricName == "node_memory_MemAvailable_bytes" {
			value = 8000000000.0 + (rand.Float64() * 4000000000.0)
		} else if metricName == "node_memory_MemFree_bytes" {
			value = 2000000000.0 + (rand.Float64() * 2000000000.0)
		} else if metricName == "node_uname_info" {
			value = 1.0
		} else if metricName == "node_time_seconds" {
			value = float64(time.Now().Unix())
		} else if strings.Contains(metricName, "bytes") {
			value = rand.Float64() * 10000000000.0
		} else if strings.Contains(metricName, "total") {
			value = rand.Float64() * 1000000.0
		} else if strings.Contains(metricName, "seconds") {
			value = rand.Float64() * 10000.0
		} else {
			value = rand.Float64() * 100.0
		}
		
		metrics = append(metrics, nodeExporterMetric{
			ts:     now,
			value:  value,
			labels: metricLabels,
		})
	}
	
	return metrics
}