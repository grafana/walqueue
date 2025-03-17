# Prometheus Queue Benchmark Results

## System Information
Latest benchmark run: 2025-03-16 14:58 UTC

- **CPU**: 13th Gen Intel(R) Core(TM) i5-13500
- **Git Commit**: b34b427
- **Previous Commit**: b34b427

## Latest Performance Results

| Test Configuration | Signals/sec | Signals/Request | Batch Size | Connections |
|-------------------|------------|-----------------|------------|-------------|
| 1000_conn_3000_batch | 824777.56 | 2310.84 | 3000 | 1000 |
| 500_conn_3000_batch | 497932.90 | 2283.27 | 3000 | 500 |
| 500_conn_6000_batch | 974289.92 | 3716.99 | 6000 | 500 |

## Historical Performance

### 1000_conn_3000_batch

| Date (UTC) | Signals/sec | Signals/Request | %Change | Git Commit |
|------------|------------|-----------------|----------|------------|
| 2025-03-16 14:58 | 824777.56 | 2310.84 | baseline | b34b427 |
| 2025-03-16 14:01 | 812749.75 | 2308.75 | ⚪ -1.46 | b34b427 |
### 500_conn_3000_batch

| Date (UTC) | Signals/sec | Signals/Request | %Change | Git Commit |
|------------|------------|-----------------|----------|------------|
| 2025-03-16 14:58 | 497932.90 | 2283.27 | baseline | b34b427 |
| 2025-03-16 14:01 | 494960.84 | 2277.05 | ⚪ -0.60 | b34b427 |
### 500_conn_6000_batch

| Date (UTC) | Signals/sec | Signals/Request | %Change | Git Commit |
|------------|------------|-----------------|----------|------------|
| 2025-03-16 14:58 | 974289.92 | 3716.99 | baseline | b34b427 |
| 2025-03-16 14:01 | 985632.79 | 3720.12 | ⚪ 1.16 | b34b427 |
