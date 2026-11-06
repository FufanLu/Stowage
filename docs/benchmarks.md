# Benchmarks

Hardware: Apple M3 Pro (12 cores), 18 GB RAM, APFS on the internal SSD, macOS 15.
Single-threaded, release build (`lto = "thin"`, `codegen-units = 1`). These are
single runs on a laptop: repeat runs of the same configuration varied by up to 2×.
Compare ratios within one run, not absolute numbers across runs.

Reproduce:

```bash
cargo build --release -p stow-bench
./target/release/stow-bench engine --engine both --keys 500000                    # 1
./target/release/stow-bench engine --engine btree --keys 500000 --checkpoint-mib 64   # 2
./target/release/stow-bench engine --engine both --keys 500000 --cache-mib 256    # 3
```

Workload: 500 000 keys (`user` + 16 hex digits, 20 B) with 100 B values, inserted in
random order with one key per batch and no fsync. Then come 200 000 uniform point
gets, 200 000 gets of missing keys, 20 000 scans of 100 keys from random starts,
and 2 000 single-key updates with fsync.

## M1 — storage engine (B+Tree vs LSM)

### 1. Default configuration (32 MiB cache, checkpoint every 256 MiB of WAL)

```text
keys 500000 · value 100 B · cache 32 MiB · single thread · aarch64

| engine | phase                |    ops/s   | p50 µs   | p99 µs   | p99.9 µs  |
|--------|----------------------|-----------:|---------:|---------:|----------:|
| btree  | load (no sync)       |      76452 |    10.01 |    50.56 |    393.21 |
| btree  | get (uniform)        |     189359 |     3.29 |    18.34 |    156.29 |
| btree  | get (missing)        |    2256932 |     0.38 |     0.58 |      0.67 |
| btree  | scan 100             |      57822 |    16.38 |    36.77 |    149.89 |
| btree  | update (fsync each)  |        194 |  5001.22 | 10059.77 |  15106.05 |
| btree  | on-disk size         |    80.0 MiB for 57.2 MiB of user data (1.40x) |

btree: height 3 · 10237 pages · 10170 leaf / 63 internal splits · WAL 172.6 MiB (10237 full-page images averaging 4118 B, 512190 deltas averaging 251 B) · 262010 page write-backs · 1 checkpoints · write amplification ≈ 38.8x · 98.0% optimistic writes
| lsm    | load (no sync)       |     239300 |     2.88 |    12.42 |     86.21 |
| lsm    | get (uniform)        |     138792 |     4.38 |    20.46 |    250.62 |
| lsm    | get (missing)        |     968722 |     0.29 |     0.46 |     38.24 |
| lsm    | scan 100             |      13950 |    33.73 |   270.33 |  10108.93 |
| lsm    | update (fsync each)  |        218 |  4050.94 | 12558.33 |  17219.58 |
| lsm    | on-disk size         |   123.8 MiB for 57.2 MiB of user data (2.16x) |
```

### 2. Checkpoint every 64 MiB of WAL

```text
keys 500000 · value 100 B · cache 32 MiB · single thread · aarch64

| engine | phase                |    ops/s   | p50 µs   | p99 µs   | p99.9 µs  |
|--------|----------------------|-----------:|---------:|---------:|----------:|
| btree  | load (no sync)       |      56925 |     8.26 |    51.10 |    518.91 |
| btree  | get (uniform)        |     171835 |     3.46 |    24.18 |    143.36 |
| btree  | get (missing)        |    1066240 |     0.38 |     0.58 |      0.67 |
| btree  | scan 100             |      29474 |    17.76 |    99.14 |    666.62 |
| btree  | update (fsync each)  |        231 |  4055.04 | 10387.45 |  16662.53 |
| btree  | on-disk size         |    80.0 MiB for 57.2 MiB of user data (1.40x) |

btree: height 3 · 10237 pages · 10170 leaf / 63 internal splits · WAL 409.9 MiB (51173 full-page images averaging 5889 B, 471254 deltas averaging 251 B) · 266090 page write-backs · 7 checkpoints · write amplification ≈ 43.5x · 98.0% optimistic writes
```

### 3. 256 MiB cache (the whole tree fits)

```text
keys 500000 · value 100 B · cache 256 MiB · single thread · aarch64

| engine | phase                |    ops/s   | p50 µs   | p99 µs   | p99.9 µs  |
|--------|----------------------|-----------:|---------:|---------:|----------:|
| btree  | load (no sync)       |     167422 |     5.25 |    23.14 |     41.98 |
| btree  | get (uniform)        |     931132 |     1.00 |     1.62 |      2.25 |
| btree  | get (missing)        |    2650357 |     0.33 |     0.42 |      0.50 |
| btree  | scan 100             |      85159 |    11.26 |    18.93 |     42.72 |
| btree  | update (fsync each)  |        224 |  4329.47 |  7139.33 |   9748.48 |
| btree  | on-disk size         |    80.0 MiB for 57.2 MiB of user data (1.40x) |

btree: height 3 · 10237 pages · 10170 leaf / 63 internal splits · WAL 172.6 MiB (10237 full-page images averaging 4118 B, 512190 deltas averaging 251 B) · 10237 page write-backs · 1 checkpoints · write amplification ≈ 4.4x · 98.0% optimistic writes
| lsm    | load (no sync)       |     315136 |     2.42 |     8.88 |     32.38 |
| lsm    | get (uniform)        |     331922 |     3.00 |     6.25 |      9.29 |
| lsm    | get (missing)        |    3417591 |     0.25 |     0.33 |      0.42 |
| lsm    | scan 100             |      36601 |    26.64 |    34.85 |     42.27 |
| lsm    | update (fsync each)  |        221 |  4143.10 |  8253.44 |  13385.73 |
| lsm    | on-disk size         |   123.8 MiB for 57.2 MiB of user data (2.16x) |
```

### 0. Before hole elision (development snapshot, for the ablation in chapter 02)

At that point full-page images were logged as whole 8 KiB pages, delta zero runs
were sent byte by byte, and the checkpoint interval was 64 MiB.

```text
| btree  | load (no sync)       |      60511 |     6.50 |    51.42 |    490.75 |
| btree  | get (uniform)        |     291227 |     3.58 |     8.00 |     13.88 |
| btree  | get (missing)        |    2498636 |     0.38 |     0.50 |      0.58 |
| btree  | scan 100             |      61278 |    16.05 |    26.22 |     36.77 |
| btree  | update (fsync each)  |        215 |  4657.15 |  8241.15 |  22986.75 |
| btree  | on-disk size         |    80.0 MiB for 57.2 MiB of user data (1.40x) |
btree: height 3 · 10237 pages · 10170 leaf / 63 internal splits · WAL 1658.5 MiB (197384 full-page images, 325043 deltas averaging 343 B) · 269902 page write-backs · 26 checkpoints · write amplification ≈ 65.8x · 98.0% optimistic writes
```

How these numbers are interpreted: [illustrated chapter 02 §2.5](illustrated/02-crash-and-recover.md#25-what-durability-costs).

## M3 — three-node cluster

`stow-bench kv` against three `stowd` processes. 100 000 keys of 100 B were
pre-split into 12 ranges, with leaders spread 4/4/4 by the scheduler. Clients
route by cached range leaders.

```bash
docker compose -f deploy/compose/docker-compose.yml up -d --build
docker compose -f deploy/compose/docker-compose.yml run --rm bench kv \
  --peers 1=n1:7001,2=n2:7001,3=n3:7001 --keys 100000 --concurrency 64 --duration 20 --read-ratio 0.5 --presplit 12
```

### Linux in Docker Desktop (fdatasync), final commit

```text
loaded 100000 keys in 87.1 s (1149 writes/s)

concurrency 64 · 100000 keys · value 100 B · read ratio 0.5 · 20 s
| op    |   ops/s | p50 ms | p99 ms | p99.9 ms |
|-------|--------:|-------:|-------:|---------:|
| read  |     864 |  32.26 | 101.84 |   157.94 |
| write |     867 |  32.72 | 110.49 |   155.45 |
total 1731 ops/s · ambiguous writes 0 · errors 0

concurrency 64 · read ratio 0.95
| read  |    2096 |  29.00 |  85.79 |   165.41 |
| write |     112 |  35.62 |  92.80 |   174.46 |
total 2208 ops/s · ambiguous writes 0 · errors 0

concurrency 256 · read ratio 0.5
| read  |    2455 |  33.95 | 497.29 |  1207.96 |
| write |    2466 |  34.18 | 416.28 |  1133.51 |
total 4922 ops/s · ambiguous writes 107 · errors 0
```

### Design iterations measured on the way

| Version | Environment | 64 clients, 50/50 |
|---|---|---|
| one Raft log file per range | macOS native | ≈60 writes/s while loading (election storms) |
| one shared Raft log per node | macOS native (`F_FULLFSYNC`) | 742 ops/s, p50 89 ms |
| shared log | Docker (fdatasync), earlier commit | 2 499 ops/s, p50 28 ms |
| shared log + pipelined fsync (reverted) | Docker | 348 ops/s, p50 132 ms, 45 ambiguous |

What each step changed: [illustrated chapter 04 §4.6](illustrated/04-a-range-splits.md#46-on-real-disks).
