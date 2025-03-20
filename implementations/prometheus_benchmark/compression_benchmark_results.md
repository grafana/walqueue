# Prometheus Metrics Compression Benchmark Results

This benchmark compares compression algorithms used in Prometheus metrics storage, specifically Snappy vs. S2 vs. Zstandard (zstd) at various compression levels using realistic node_exporter metrics.

## Test Parameters

- **Test Date**: March 19, 2025
- **Metrics Type**: Simulated node_exporter metrics with realistic label cardinality
- **Total Metrics**: 1,000,000 metrics
- **Batch Size**: 100,000 metrics per commit
- **Compression Algorithms Tested**:
  - Snappy: Current default in Prometheus
  - S2: Optimized successor to Snappy by Klauspost
  - Zstd (zstandard): With three different compression levels
    - Fastest (level 1): Speed-optimized
    - Default (level 3): Balanced compression/speed
    - Better (level 7): Compression-optimized

## Compression Results

| Compression Format | Compression Ratio | Avg Compression Time | Size Reduction vs Snappy |
|-------------------|------------------|---------------------|-------------------------|
| Snappy             | 23.38%           | 8.58 ms              | Baseline                |
| S2                 | 20.55%           | 8.87 ms              | 12.22% smaller         |
| Zstd-Fastest       | 17.25%           | 26.20 ms             | 26.17% smaller         |
| Zstd-Default       | 15.99%           | 29.06 ms             | 31.48% smaller         |
| Zstd-Better        | 14.13%           | 45.45 ms             | 39.72% smaller         |

## Decompression Results

| Compression Format | Decompression Time | vs Snappy    |
|-------------------|-------------------|--------------|
| Snappy             | 4.85 ms            | Baseline     |
| S2                 | 4.34 ms            | 1.12x faster |
| Zstd-Fastest       | 7.31 ms            | 1.51x slower |
| Zstd-Default       | 6.65 ms            | 1.37x slower |
| Zstd-Better        | 5.96 ms            | 1.23x slower |

## Memory Usage

| Compression Format | Memory Used     | Output Size   | Compression Ratio |
|-------------------|----------------|---------------|-------------------|
| Uncompressed       | Baseline       | 22.2 MB       | 100.00%           |
| Snappy             | 25.9 MB        | 5.2 MB        | 23.40%            |
| S2                 | 22.3 MB        | 4.6 MB        | 20.58%            |
| Zstd-Fastest       | 29.7 MB        | 3.8 MB        | 17.19%            |
| Zstd-Better        | 120.8 MB       | 3.1 MB        | 14.09%            |
