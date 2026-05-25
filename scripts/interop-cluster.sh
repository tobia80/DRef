#!/usr/bin/env bash
# Drive the cross-language DRef interop cluster (2 Scala + 1 Rust Raft nodes)
# defined in docker-compose.interop.yml.
#
# Usage:
#   scripts/interop-cluster.sh up        # build images and start the cluster
#   scripts/interop-cluster.sh status    # list containers + show current leader
#   scripts/interop-cluster.sh logs      # follow logs from every node
#   scripts/interop-cluster.sh attach N  # attach to scala-node-N or rust-node-N
#   scripts/interop-cluster.sh attach rust|scala
#   scripts/interop-cluster.sh down      # stop and remove containers + network

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

COMPOSE_FILE="docker-compose.interop.yml"
PROJECT_NAME="dref-interop"
SCALA_REPLICAS="${SCALA_REPLICAS:-2}"
RUST_REPLICAS="${RUST_REPLICAS:-1}"

dc() {
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" "$@"
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker is required but not on PATH" >&2
    exit 1
  fi
  if ! docker compose version >/dev/null 2>&1; then
    echo "error: 'docker compose' plugin is required (v2)." >&2
    exit 1
  fi
}

cmd_up() {
  require_docker
  echo "==> building Scala + Rust images (first run takes a few minutes)"
  dc build
  echo "==> starting cluster: $SCALA_REPLICAS Scala node(s) + $RUST_REPLICAS Rust node(s)"
  dc up -d --no-build \
    --scale "scala-node=${SCALA_REPLICAS}" \
    --scale "rust-node=${RUST_REPLICAS}"

  echo
  echo "==> cluster is starting. Give Raft a few seconds to elect a leader,"
  echo "    then attach to a node to chat:"
  echo
  echo "    scripts/interop-cluster.sh status"
  echo "    scripts/interop-cluster.sh attach scala   # any Scala node"
  echo "    scripts/interop-cluster.sh attach rust    # the Rust node"
  echo "    scripts/interop-cluster.sh attach 1       # specific replica (1-based)"
  echo "    scripts/interop-cluster.sh logs           # follow all logs"
  echo
  echo "Detach from an attached container with Ctrl-p Ctrl-q (keeps it running)."
  echo "Stop everything with: scripts/interop-cluster.sh down"
}

cmd_down() {
  require_docker
  echo "==> stopping cluster and removing containers + network"
  dc down --remove-orphans
}

cmd_status() {
  require_docker
  echo "==> containers:"
  dc ps
  echo
  echo "==> tail of recent logs (last 5 lines per node):"
  dc logs --tail 5 || true
}

cmd_logs() {
  require_docker
  dc logs -f
}

pick_container() {
  # Resolve the user's "scala", "rust", or "<n>" argument into a container name.
  local target="$1"
  local -a scala_ids rust_ids
  mapfile -t scala_ids < <(dc ps -q scala-node)
  mapfile -t rust_ids  < <(dc ps -q rust-node)

  resolve() {
    docker inspect --format '{{.Name}}' "$1" 2>/dev/null | sed 's#^/##'
  }

  case "$target" in
    scala)
      [ "${#scala_ids[@]}" -gt 0 ] || { echo "no scala-node containers running" >&2; return 1; }
      resolve "${scala_ids[0]}"
      ;;
    rust)
      [ "${#rust_ids[@]}" -gt 0 ] || { echo "no rust-node containers running" >&2; return 1; }
      resolve "${rust_ids[0]}"
      ;;
    ''|*[!0-9]*)
      echo "unknown target '$target' (use 'scala', 'rust', or a 1-based index)" >&2
      return 1
      ;;
    *)
      local idx=$((target - 1))
      local -a all_ids=("${scala_ids[@]}" "${rust_ids[@]}")
      if [ "$idx" -lt 0 ] || [ "$idx" -ge "${#all_ids[@]}" ]; then
        echo "index $target out of range (1..${#all_ids[@]})" >&2
        return 1
      fi
      resolve "${all_ids[$idx]}"
      ;;
  esac
}

cmd_attach() {
  require_docker
  if [ "$#" -lt 1 ]; then
    echo "usage: $0 attach <scala|rust|N>" >&2
    exit 2
  fi
  local container
  container="$(pick_container "$1")"
  echo "==> attaching to ${container} (detach with Ctrl-p Ctrl-q)"
  docker attach "$container"
}

cmd_help() {
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-help}" in
  up|start)        shift; cmd_up "$@" ;;
  down|stop)       shift; cmd_down "$@" ;;
  status|ps)       shift; cmd_status "$@" ;;
  logs)            shift; cmd_logs "$@" ;;
  attach)          shift; cmd_attach "$@" ;;
  help|-h|--help)  cmd_help ;;
  *)               echo "unknown command: $1" >&2; cmd_help; exit 2 ;;
esac
