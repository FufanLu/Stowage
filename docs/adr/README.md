# Architecture Decision Records

| ADR | Decision |
|---|---|
| [0001](0001-deterministic-simulation.md) | Deterministic simulation from day one |
| [0002](0002-rust.md) | Rust as the implementation language |
| [0003](0003-layered-cp-architecture.md) | One transactional KV, with SQL and document layers on top (CP) |
| [0004](0004-btree-redo-logging.md) | B+Tree storage engine with redo-only mini-transaction logging |
| [0005](0005-raft-as-a-pure-state-machine.md) | Hand-written Raft as a pure state machine |

New ADRs copy the shape of the existing ones: Context, Decision, Consequences,
Alternatives considered.
