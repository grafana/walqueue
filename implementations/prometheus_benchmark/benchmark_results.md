# Prometheus Queue Benchmark Results

## System Information
Latest benchmark run: 2025-03-19 15:46 UTC

- **CPU**: 13th Gen Intel(R) Core(TM) i5-13500
- **Git Commit**: b75b12c
- **Previous Commit**: b34b427

## Latest Performance Results

| Test Configuration | Signals/sec | Signals/Request | Batch Size | Connections |
|-------------------|------------|-----------------|------------|-------------|
| 1000_conn_3000_batch | 917741.14 | 2447.08 | 3000 | 1000 |
| 500_conn_3000_batch | 571169.60 | 2427.86 | 3000 | 500 |
| 500_conn_6000_batch | 1104694.29 | 4114.20 | 6000 | 500 |

## Historical Performance

### 1000_conn_3000_batch

| Date (UTC) | Signals/sec | Signals/Request | %Change | Git Commit |
|------------|------------|-----------------|----------|------------|
| 2025-03-19 15:46 | 917741.14 | 2447.08 | baseline | b75b12c |
| 2025-03-16 14:58 | 824777.56 | 2310.84 | 🔴 -10.13 | b34b427 |
| 2025-03-16 14:01 | 812749.75 | 2308.75 | ⚪ -1.46 | b34b427 |
### 500_conn_3000_batch

| Date (UTC) | Signals/sec | Signals/Request | %Change | Git Commit |
|------------|------------|-----------------|----------|------------|
| 2025-03-19 15:46 | 571169.60 | 2427.86 | baseline | b75b12c |
| 2025-03-16 14:58 | 497932.90 | 2283.27 | 🔴 -12.82 | b34b427 |
| 2025-03-16 14:01 | 494960.84 | 2277.05 | ⚪ -0.60 | b34b427 |
### 500_conn_6000_batch

| Date (UTC) | Signals/sec | Signals/Request | %Change | Git Commit |
|------------|------------|-----------------|----------|------------|
| 2025-03-19 15:46 | 1104694.29 | 4114.20 | baseline | b75b12c |
| 2025-03-16 14:58 | 974289.92 | 3716.99 | 🔴 -11.80 | b34b427 |
| 2025-03-16 14:01 | 985632.79 | 3720.12 | ⚪ 1.16 | b34b427 |
