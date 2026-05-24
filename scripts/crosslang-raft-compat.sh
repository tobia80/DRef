#!/usr/bin/env bash
# Cross-language Raft protobuf compatibility: wire-format tests on both sides.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Scala: consensus protobuf golden bytes"
sbt "dref-raft/testOnly io.github.tobia80.dref.raft.CrossLangConsensusCompatSpec"

echo "==> Rust: consensus protobuf golden bytes"
(cd "$ROOT/rust" && cargo test -p dref-raft --test consensus_compat_tests)

echo "==> Scala: proto-backed Raft cluster"
sbt "dref-raft/testOnly io.github.tobia80.dref.raft.ProtoRaftDRefSpec"

echo "==> Rust: proto-backed Raft cluster"
(cd "$ROOT/rust" && cargo test -p dref-raft --test raft_tests)

echo "==> Cross-language Raft protobuf compatibility OK"
