# ADR 0001: Deterministic simulation from day one

- Status: accepted
- Date: 2026-11-06

## Context

Most of Stowage's hard bugs will be in Raft, range splits and distributed transactions.
They show up only under specific interleavings: a message delayed past an election
timeout, a crash between `write` and `fsync`, a partition that heals while a leader
lease is still valid. Integration tests on real processes find these by luck, and a
failure seen once in CI usually cannot be reproduced.

FoundationDB, TigerBeetle and AWS (the S3 ShardStore and Aurora DSQL teams) all
handle this the same way: run the real protocol code in a single-threaded simulator
whose only source of non-determinism is one seeded RNG. A failure then reduces to a
seed, and re-running that seed replays it event for event.

This only works if the protocol code never reaches for the OS directly. Retrofitting
that later means rewriting every component, so the decision has to be made before
the first protocol line is written.

## Decision

1. **Four environment traits** in `stowage-common::env`: `Clock`, `Rng`, `Disk` (+
   `DiskFile`) and `Transport`. Stateful components (storage engine, Raft, txn
   coordinator, placement driver) take these as parameters. They never call
   `std::time`, `std::fs`, `rand` or sockets themselves.
2. **Protocol cores are event handlers.** Raft and the other protocols are written
   as `on_message` / `on_timer` state machines that emit messages and timers. The sim
   drives them through `stowage_sim::Actor`. Production drives the same code from tokio
   tasks over tonic.
3. **The simulator models real failure semantics**, not idealised ones:
   - latency is uniform over a range, which also reorders messages;
   - messages can be dropped and duplicated;
   - partitions also cut messages that are already in flight;
   - a crash kills a node's pending timers;
   - file contents survive only up to the last `sync()`, and creates, removes and
     renames survive only after `sync_dir()`;
   - in `CrashMode::Torn`, an unsynced write can survive whole, survive torn at a
     512-byte sector boundary, or be lost.
4. **Every processed event is hashed** into a trace fingerprint. A test asserts that
   the same seed produces the same fingerprint, which catches accidental
   non-determinism (a `HashMap` iteration, a stray `SystemTime::now()`).
5. **Seed protocol.** Each test runs a fixed seed list. `STOWAGE_SIM_SEEDS=N` adds N
   random seeds; CI's `simulation` job runs 200 per push. `STOWAGE_SIM_SEED=n` runs
   exactly one seed. On failure, `for_each_seed` prints the reproducing command.
6. **Naming convention.** Simulation tests live in `tests/simulation.rs` of their
   crate, so `nextest -E 'binary(simulation)'` selects them all.

## Consequences

- Every component gets an extra layer of indirection (`Arc<dyn Disk>`, `Arc<dyn Clock>`).
  This costs a virtual call per I/O, which is negligible next to the I/O itself.
- Async code must keep protocol logic out of `async fn`s that await the network. The
  async layer is a thin adapter around a synchronous core. That also makes the core
  easier to unit-test.
- The simulator is single-threaded, so it cannot find data races. Those are covered
  separately by `loom` (B+Tree latches, M1) and by running the real binaries under
  chaos in compose (M9).

## Alternatives considered

- **madsim / turmoil** (tokio-compatible simulators): convenient, but they intercept
  tokio rather than force an explicit environment boundary. Our own simulator also has
  to model disk durability (sync vs. dir-sync, torn writes), which those do not cover
  fully.
- **Jepsen-only testing**: valuable and kept for M9, but non-reproducible and slow.
  It is a complement, not a replacement.
