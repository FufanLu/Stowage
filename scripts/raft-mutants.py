#!/usr/bin/env python3
"""Mutation testing for the Raft core.

Plants one classic Raft bug at a time into crates/raft/src/raft.rs, runs the
simulation and scenario tests, records whether the suite caught it, and restores
the file. See docs/illustrated/03-one-write-through-raft.md, section 3.6.

    python3 scripts/raft-mutants.py          # STOWAGE_SIM_SEEDS=20 by default
"""
import os, shutil, subprocess, sys, tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "crates/raft/src/raft.rs")
# Refuse to run on uncommitted changes: git is the backstop if this script dies
# mid-mutation (restore with `git checkout -- crates/raft/src/raft.rs`).
dirty = subprocess.run(["git", "status", "--porcelain", "--", SRC], cwd=ROOT, capture_output=True, text=True).stdout
if dirty.strip():
    sys.exit(f"{SRC} has uncommitted changes; commit first")
ORIG = os.path.join(tempfile.mkdtemp(), "raft.rs.orig")
shutil.copy(SRC, ORIG)

MUTANTS = {
  "figure8: commit old-term entries by counting": (
    "if candidate > self.commit && self.store.term(candidate) == Some(self.term) {",
    "if candidate > self.commit {"),
  "vote/term not flushed before replying": (
    "        let r = f(self);\n        self.flush_hard_state()?;\n        r",
    "        f(self)"),
  "lease without leader stickiness": (
    "        self.cfg.check_quorum\n            && self.leader.is_some()\n            && self.election_elapsed < self.cfg.election_ticks",
    "        false"),
  "read before own-term commit": (
    "        if self.store.term(self.commit) != Some(self.term) {\n            // Our no-op",
    "        if false {\n            // Our no-op"),
  "no election restriction": (
    "        last_term > my_term || (last_term == my_term && last_index >= self.store.last_index())",
    "        let _ = (last_index, last_term, my_term);\n        true"),
  "no log-matching check on append": (
    "        if self.store.term(prev_index) != Some(prev_term) {\n            let last",
    "        if self.store.term(prev_index).is_none() && prev_index > 0 {\n            let last"),
}
results = {}
original = open(ORIG).read()
missing = [name for name, (old, _) in MUTANTS.items() if old not in original]
if missing:
    sys.exit(f"patterns not found (code was reformatted?): {missing}")
try:
    for name, (old, new) in MUTANTS.items():
        open(SRC, "w").write(original.replace(old, new, 1))
        env = dict(os.environ, STOWAGE_SIM_SEEDS=os.environ.get("STOWAGE_SIM_SEEDS", "20"))
        p = subprocess.run(["cargo", "nextest", "run", "--release", "-p", "stowage-raft", "-E", "binary(simulation) or binary(scenarios)", "--no-fail-fast"],
                           cwd=ROOT, env=env, capture_output=True, text=True)
        out = p.stdout + p.stderr
        fails = sorted({l.split("stowage-raft::")[-1].strip() for l in out.splitlines() if l.strip().startswith("FAIL [")})
        compiled = "error[E" not in out and "could not compile" not in out
        caught = p.returncode != 0 and compiled and bool(fails)
        results[name] = (caught, fails)
        status = "CAUGHT" if caught else ("BUILD-ERROR" if not compiled else "MISSED")
        print(f"{status:7} {name}: {fails}", flush=True)
finally:
    shutil.copy(ORIG, SRC)
    print("restored", SRC)
sys.exit(0 if results and all(r[0] for r in results.values()) else 1)
