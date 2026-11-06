.PHONY: check fmt lint test sim deny trace

check: fmt lint test deny

fmt:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo nextest run --workspace

# Explore N fresh random seeds; reproduce a failure with STOWAGE_SIM_SEED=<seed>.
sim:
	STOWAGE_SIM_SEEDS=$${N:-200} cargo nextest run --workspace --release -E 'binary(simulation)'

deny:
	cargo deny check

# Real trace used in docs/illustrated/A-deterministic-simulation.md
trace:
	cargo run -q -p stowage-sim --example trace -- $${SEED:-42}
