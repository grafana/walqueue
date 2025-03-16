//go:generate go run benchmark_report.go
package prometheus_benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// BenchmarkResult represents a single benchmark result
type BenchmarkResult struct {
	TestName             string  `json:"test_name"`
	BatchSize            int     `json:"batch_size"`
	MaxConnections       uint    `json:"max_connections"`
	SignalCount          int     `json:"signal_count"`
	DurationSec          float64 `json:"duration_sec"`
	SignalsPerSec        float64 `json:"signals_per_sec"`
	RequestCount         int64   `json:"request_count"`
	SignalsPerReq        float64 `json:"signals_per_request"`
	AppenderWritesPerSec float64 `json:"appender_writes_per_sec"`
	NumCPU               int     `json:"num_cpu"`
	GOMAXPROCS           int     `json:"gomaxprocs"`
	CPUModel             string  `json:"cpu_model"`
	Timestamp            string  `json:"timestamp"`
}

// BenchmarkResultsWithMeta represents the benchmark results with metadata
type BenchmarkResultsWithMeta struct {
	Results  []BenchmarkResult `json:"results"`
	Metadata struct {
		GitCommit    string    `json:"git_commit"`
		Timestamp    time.Time `json:"timestamp"`
		GoVersion    string    `json:"go_version"`
		NumCPU       int       `json:"num_cpu"`
		GOMAXPROCS   int       `json:"gomaxprocs"`
		CPUModel     string    `json:"cpu_model"`
		TotalSignals int64     `json:"total_signals"`
	} `json:"metadata"`
}

// BenchmarkSummary contains the key information from a benchmark run
type BenchmarkSummary struct {
	Date      string
	GitCommit string
	CPUModel  string
	Results   map[string]BenchmarkResult
	Timestamp time.Time
}

func main() {
	fmt.Println("Generating benchmark report to benchmark_results.md...")
	// Find all benchmark files
	files, err := filepath.Glob("benchmark_results/prometheus_*.json")
	if err != nil {
		fmt.Printf("Error finding benchmark files: %v\n", err)
		os.Exit(1)
	}

	if len(files) == 0 {
		fmt.Printf("No benchmark files found in benchmark_results/\n")
		os.Exit(1)
	}

	// Parse all benchmark files
	summaries := make([]BenchmarkSummary, 0, len(files))
	testCases := make(map[string]bool)

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			fmt.Printf("Error reading %s: %v\n", file, err)
			continue
		}

		var result BenchmarkResultsWithMeta
		if err := json.Unmarshal(data, &result); err != nil {
			fmt.Printf("Error parsing %s: %v\n", file, err)
			continue
		}

		// Extract date from filename (format: prometheus_20250316_095754.json)
		dateStr := strings.Split(filepath.Base(file), "_")[1]

		// Create a summary for this benchmark
		summary := BenchmarkSummary{
			Date:      formatDate(dateStr),
			GitCommit: result.Metadata.GitCommit,
			CPUModel:  result.Metadata.CPUModel,
			Results:   make(map[string]BenchmarkResult),
			Timestamp: result.Metadata.Timestamp,
		}

		// Store the results by test name
		for _, r := range result.Results {
			testCases[r.TestName] = true
			summary.Results[r.TestName] = r
		}

		summaries = append(summaries, summary)
	}

	// Sort summaries by date (most recent first)
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Timestamp.After(summaries[j].Timestamp)
	})

	// Extract unique test names and sort them
	tests := make([]string, 0, len(testCases))
	for test := range testCases {
		tests = append(tests, test)
	}
	sort.Strings(tests)

	// Generate markdown table
	generateMarkdownTable(summaries, tests)
}

// formatDate converts date string from 20250316 to 2025-03-16
func formatDate(dateStr string) string {
	if len(dateStr) != 8 {
		return dateStr
	}
	return fmt.Sprintf("%s-%s-%s", dateStr[0:4], dateStr[4:6], dateStr[6:8])
}

func generateMarkdownTable(summaries []BenchmarkSummary, tests []string) {
	// Create or truncate the output file
	outputFile, err := os.Create("benchmark_results.md")
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer outputFile.Close()

	// Write the table header
	writeLine := func(format string, args ...interface{}) {
		fmt.Fprintf(outputFile, format+"\n", args...)
	}

	writeLine("# Prometheus Queue Benchmark Results\n")
	writeLine("## System Information")

	if len(summaries) > 0 {
		latestDate := summaries[0].Date
		if !summaries[0].Timestamp.IsZero() {
			latestDate = fmt.Sprintf("%s %s UTC",
				summaries[0].Date,
				summaries[0].Timestamp.UTC().Format("15:04"))
		}
		writeLine("Latest benchmark run: %s\n", latestDate)
		writeLine("- **CPU**: %s", summaries[0].CPUModel)
		writeLine("- **Git Commit**: %s", summaries[0].GitCommit)
		if len(summaries) > 1 {
			writeLine("- **Previous Commit**: %s", summaries[1].GitCommit)
		}
		writeLine("")
	}

	// Get the most recent benchmark run
	latestSummary := summaries[0]

	// Group results by test configuration
	writeLine("## Latest Performance Results\n")

	// Table headers
	writeLine("| Test Configuration | Signals/sec | Signals/Request | Batch Size | Connections |")
	writeLine("|-------------------|------------|-----------------|------------|-------------|")

	// Table rows for latest results only
	for _, test := range tests {
		if result, ok := latestSummary.Results[test]; ok {
			writeLine("| %s | %.2f | %.2f | %d | %d |",
				result.TestName,
				result.SignalsPerSec,
				result.SignalsPerReq,
				result.BatchSize,
				result.MaxConnections)
		}
	}

	// Historical data section
	writeLine("\n## Historical Performance\n")

	// For each test configuration, show the performance over time
	for _, test := range tests {
		// Check if this test appears in enough benchmarks
		testCount := 0
		for _, summary := range summaries {
			if _, ok := summary.Results[test]; ok {
				testCount++
			}
		}

		if testCount < 2 {
			continue // Skip tests that don't have enough historical data
		}

		writeLine("### %s\n", test)

		// Table headers for historical data
		writeLine("| Date (UTC) | Signals/sec | Signals/Request | %%Change | Git Commit |")
		writeLine("|------------|------------|-----------------|----------|------------|")

		// Track previous value for calculating percent change
		var prevValue float64

		// Show all historical results for this test
		for i, summary := range summaries {
			if result, ok := summary.Results[test]; ok {
				// Calculate percent change
				var changeStr string
				// Add color indicator based on change
				indicator := ""
				if i == 0 {
					// First result is baseline
					changeStr = "baseline"
					prevValue = result.SignalsPerSec
				} else {
					// Calculate change from previous run
					change := 100 * (result.SignalsPerSec - prevValue) / prevValue

					if change > 5.0 {
						indicator = "🟢 "
					} else if change < -5.0 {
						indicator = "🔴 "
					} else {
						indicator = "⚪ "
					}
					changeStr = fmt.Sprintf("%.2f", change)
				}

				changeStr = fmt.Sprintf("%s%s", indicator, changeStr)
				prevValue = result.SignalsPerSec

				// Format date with UTC time (without seconds)
				dateDisplay := summary.Date
				if !summary.Timestamp.IsZero() {
					dateDisplay = fmt.Sprintf("%s %s",
						summary.Date,
						summary.Timestamp.UTC().Format("15:04"))
				}

				writeLine("| %s | %.2f | %.2f | %s | %s |",
					dateDisplay,
					result.SignalsPerSec,
					result.SignalsPerReq,
					changeStr,
					summary.GitCommit)
			}
		}
	}

	fmt.Printf("Benchmark report written to benchmark_results.md\n")
}
