# Benchmark

## Setup

**Hardware**:

| Component | Detail |
|-----------|--------|
| OS  | Ubuntu 25.10 |
| CPU | 12th Gen Intel Core i5-12600K (16 threads) |
| RAM | 64 GB DDR4 |
| Scylla disk | Samsung 980 Pro 2 TB NVMe |
| circe disk | Samsung 970 EVO Plus 2 TB NVMe |
| fs.aio-max-nr | 1048576 |


**Dataset**: `bench.events`, 40M rows, ~200 bytes/row logical, 3.8 GiB on-disk (LZ4).
`nodetool compact bench` was run after populating the table.

```sql
CREATE TABLE bench.events (
    id        uuid,      -- random, uniform token distribution
    ts        timestamp,
    device_id text,      -- 10,000 distinct values
    user_id   text,      -- 100,000 distinct values
    event     text,      -- 20 distinct values
    value     double,
    count     int,
    active    boolean,
    PRIMARY KEY (id)
)
```

## Concurrency and Splits

Formula from the [ScyllaDB full table scan post][scylla-scan]:

```
N (concurrency) = nodes × cores_per_node × 3
M (splits)      = N × 100
```

For this setup (1 node, 8 Scylla cores):

```
N = 1 × 8 × 3 = 24
M = 24 × 100  = 2400
```

## Results

All runs produce single-file Parquet output (`-s`), 1.5 GiB.

| Splits | Concurrency | Wall time | CPU% | User time |
|--------|-------------|-----------|------|-----------|
| 16     | 16          | 1m 08.8s  | 81%  | 54.3s     |
| 256    | 16          | 53.8s     | 113% | 59.0s     |
| 256    | 24          | 47.7s     | 136% | 62.7s     |
| 2400   | 24          | **32.6s** | 227% | 71.2s     |

## Observations

- Default coupled split/concurrency (16/16): 81% CPU indicates Tokio workers waiting on long-tail range completions — Scylla shards sat idle between round trips.
- Decoupling splits from concurrency (work-stealing queue) gave the largest single improvement: 256/16 cut wall time ~22% over 16/16.
- The ScyllaDB blog formula (2400/24) nearly doubled throughput vs the naive default.
- Final throughput: ~1.23M rows/sec. Ingestion was ~83K rows/sec (40M rows in 8 min at 32 workers) — export is ~15× faster than ingestion.
- Output is 1.5 GiB from a 3.8 GiB LZ4 source — additional ~2.5× columnar compression.

## Final Command

```bash
export TOKIO_WORKER_THREADS=4 && time circe -sio bench_events.parquet token-range bench.events -K id -C 24 -S 2400
```

[scylla-scan]: https://www.scylladb.com/2017/02/13/efficient-full-table-scans-with-scylla-1-6/
