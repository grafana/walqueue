# Prometheus Queue Benchmark Results

## System Information
Latest benchmark run: 2025-03-20 13:45 UTC

- **CPU**: 13th Gen Intel(R) Core(TM) i5-13500
- **Git Commit**: 729560c
- **Previous Commit**: 729560c

## Latest Performance Results

| Test Configuration | Signals/sec | Signals/Request | Batch Size | Connections |
|-------------------|------------|-----------------|------------|-------------|
| 1000_conn_3000_batch | 1288261.04 | 2118.49 | 3000 | 1000 |
| 500_conn_3000_batch | 878222.75 | 2707.39 | 3000 | 500 |
| 500_conn_6000_batch | 1293360.13 | 4274.95 | 6000 | 500 |

## Historical Performance

### 1000_conn_3000_batch

| Date (UTC) | Signals/sec | Signals/Request | %Change | Git Commit |
|------------|------------|-----------------|----------|------------|
| 2025-03-20 13:45 | 1288261.04 | 2118.49 | baseline | 729560c |
| 2025-03-16 14:58 | 824777.56 | 2310.84 | 🔴 -35.98 | b34b427 |
| 2025-03-16 14:01 | 812749.75 | 2308.75 | ⚪ -1.46 | b34b427 |
### 500_conn_3000_batch

| Date (UTC) | Signals/sec | Signals/Request | %Change | Git Commit |
|------------|------------|-----------------|----------|------------|
| 2025-03-20 13:45 | 878222.75 | 2707.39 | baseline | 729560c |
| 2025-03-16 14:58 | 497932.90 | 2283.27 | 🔴 -43.30 | b34b427 |
| 2025-03-16 14:01 | 494960.84 | 2277.05 | ⚪ -0.60 | b34b427 |
### 500_conn_6000_batch

| Date (UTC) | Signals/sec | Signals/Request | %Change | Git Commit |
|------------|------------|-----------------|----------|------------|
| 2025-03-20 13:45 | 1293360.13 | 4274.95 | baseline | 729560c |
| 2025-03-16 14:58 | 974289.92 | 3716.99 | 🔴 -24.67 | b34b427 |
| 2025-03-16 14:01 | 985632.79 | 3720.12 | ⚪ 1.16 | b34b427 |
