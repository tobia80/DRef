#!/usr/bin/env bash
# Cross-language Redis compatibility: Scala writes, Rust reads.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Scala: write cross-language Redis fixtures"
sbt "dref-redis/testOnly io.github.tobia80.dref.redis.CrossLangRedisSpec"

echo "==> Rust: verify cross-language Redis fixtures"
(cd "$ROOT/rust" && cargo test -p dref-redis --features test-redis --test crosslang_redis_tests -- --ignored --nocapture)

echo "==> Cross-language Redis compatibility OK"
