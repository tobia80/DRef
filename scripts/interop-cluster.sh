#!/usr/bin/env bash
# Drive the cross-language DRef interop cluster (Scala + Rust Raft nodes)
# defined in docker-compose.interop.yml.
#
# Usage:
#   scripts/interop-cluster.sh up              # build images and start the cluster
#   scripts/interop-cluster.sh status          # list containers + recent logs
#   scripts/interop-cluster.sh list            # show scala/rust replicas with indices
#   scripts/interop-cluster.sh add scala [N]   # add N Scala nodes (default 1)
#   scripts/interop-cluster.sh add rust [N]    # add N Rust nodes (default 1)
#   scripts/interop-cluster.sh remove scala [N]  # remove replica N (1-based; default last)
#   scripts/interop-cluster.sh remove rust [N]
#   scripts/interop-cluster.sh interactive     # REPL to add/remove/status/attach
#   scripts/interop-cluster.sh logs              # follow logs from every node
#   scripts/interop-cluster.sh attach N          # attach to scala-node-N or rust-node-N
#   scripts/interop-cluster.sh attach rust|scala
#   scripts/interop-cluster.sh down              # stop and remove containers + network

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

service_for_kind() {
  case "$1" in
    scala) echo "scala-node" ;;
    rust)  echo "rust-node" ;;
    *)     echo "unknown kind '$1' (use 'scala' or 'rust')" >&2; return 1 ;;
  esac
}

# macOS ships Bash 3.2, which has no `mapfile` — read line-by-line instead.
collect_ids() {
  local service="$1"
  local -a ids=()
  while IFS= read -r line; do
    [ -n "$line" ] && ids+=("$line")
  done < <(dc ps -q "$service" 2>/dev/null || true)
  printf '%s\n' "${ids[@]}"
}

replica_count() {
  local service="$1"
  collect_ids "$service" | grep -c . || true
}

# Compose applies --scale per service independently. If you only pass
# --scale scala-node=N, rust-node reverts to the compose-file default (1)
# and extra Rust replicas are removed. Always set both counts together.
scale_cluster() {
  local scala_count="$1"
  local rust_count="$2"
  if [ "$scala_count" -lt 0 ] || [ "$rust_count" -lt 0 ]; then
    echo "error: replica counts must be non-negative" >&2
    return 1
  fi
  if [ "$scala_count" -eq 0 ] && [ "$rust_count" -eq 0 ]; then
    echo "error: cluster must keep at least one node running" >&2
    return 1
  fi
  dc up -d --no-recreate \
    --scale "scala-node=${scala_count}" \
    --scale "rust-node=${rust_count}"
}

resolve_container_name() {
  docker inspect --format '{{.Name}}' "$1" 2>/dev/null | sed 's#^/##'
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
  echo "==> cluster is starting. Give Raft a few seconds to elect a leader."
  echo "    Nodes refresh membership every few seconds — new replicas join"
  echo "    without restarting the rest of the cluster."
  echo
  cmd_list
  echo
  echo "    scripts/interop-cluster.sh interactive   # add/remove nodes at runtime"
  echo "    scripts/interop-cluster.sh attach scala"
  echo "    scripts/interop-cluster.sh logs"
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

cmd_list() {
  require_docker
  local -a scala_ids=() rust_ids=()
  while IFS= read -r line; do
    [ -n "$line" ] && scala_ids+=("$line")
  done < <(dc ps -q scala-node 2>/dev/null || true)
  while IFS= read -r line; do
    [ -n "$line" ] && rust_ids+=("$line")
  done < <(dc ps -q rust-node 2>/dev/null || true)

  echo "==> cluster members (DNS alias dref-interop resolves all replicas):"
  local i=1
  for id in "${scala_ids[@]}"; do
    echo "  scala[$i]  $(resolve_container_name "$id")"
    i=$((i + 1))
  done
  if [ "${#scala_ids[@]}" -eq 0 ]; then
    echo "  (no scala-node containers)"
  fi
  i=1
  for id in "${rust_ids[@]}"; do
    echo "  rust[$i]   $(resolve_container_name "$id")"
    i=$((i + 1))
  done
  if [ "${#rust_ids[@]}" -eq 0 ]; then
    echo "  (no rust-node containers)"
  fi
}

cmd_add() {
  require_docker
  if [ "$#" -lt 1 ]; then
    echo "usage: $0 add <scala|rust> [count]" >&2
    exit 2
  fi
  local kind="$1"
  local count="${2:-1}"
  if ! [[ "$count" =~ ^[0-9]+$ ]] || [ "$count" -lt 1 ]; then
    echo "count must be a positive integer" >&2
    exit 2
  fi
  local service
  service="$(service_for_kind "$kind")"
  local scala_count rust_count
  scala_count="$(replica_count scala-node)"
  rust_count="$(replica_count rust-node)"
  local current new_scala new_rust
  case "$service" in
    scala-node)
      current="$scala_count"
      new_scala=$((scala_count + count))
      new_rust="$rust_count"
      ;;
    rust-node)
      current="$rust_count"
      new_scala="$scala_count"
      new_rust=$((rust_count + count))
      ;;
  esac
  echo "==> scaling $service: $current -> $((current + count)) (scala-node=${new_scala} rust-node=${new_rust})"
  scale_cluster "$new_scala" "$new_rust"
  echo "==> wait a few seconds for DNS + Raft peer refresh, then check logs:"
  cmd_list
}

cmd_remove() {
  require_docker
  if [ "$#" -lt 1 ]; then
    echo "usage: $0 remove <scala|rust> [index]" >&2
    exit 2
  fi
  local kind="$1"
  local idx="${2:-}"
  local service
  service="$(service_for_kind "$kind")"

  local -a ids=()
  while IFS= read -r line; do
    [ -n "$line" ] && ids+=("$line")
  done < <(dc ps -q "$service" 2>/dev/null || true)

  if [ "${#ids[@]}" -eq 0 ]; then
    echo "no $service containers running" >&2
    exit 1
  fi

  local target_id
  if [ -z "$idx" ]; then
    target_id="${ids[$((${#ids[@]} - 1))]}"
    idx="${#ids[@]}"
  else
    if ! [[ "$idx" =~ ^[0-9]+$ ]]; then
      echo "index must be a positive integer (see: $0 list)" >&2
      exit 2
    fi
    local zero=$((idx - 1))
    if [ "$zero" -lt 0 ] || [ "$zero" -ge "${#ids[@]}" ]; then
      echo "index $idx out of range for $kind (1..${#ids[@]})" >&2
      exit 1
    fi
    target_id="${ids[$zero]}"
  fi

  local name
  name="$(resolve_container_name "$target_id")"
  echo "==> stopping $kind replica #$idx ($name)"
  docker stop "$target_id" >/dev/null
  docker rm "$target_id" >/dev/null

  local scala_count rust_count
  scala_count="$(replica_count scala-node)"
  rust_count="$(replica_count rust-node)"
  echo "==> reconciling cluster scale (scala-node=${scala_count} rust-node=${rust_count})"
  scale_cluster "$scala_count" "$rust_count"
  cmd_list
}

pick_container() {
  local target="$1"
  local -a scala_ids=() rust_ids=()
  while IFS= read -r line; do
    [ -n "$line" ] && scala_ids+=("$line")
  done < <(dc ps -q scala-node)
  while IFS= read -r line; do
    [ -n "$line" ] && rust_ids+=("$line")
  done < <(dc ps -q rust-node)

  case "$target" in
    scala)
      [ "${#scala_ids[@]}" -gt 0 ] || { echo "no scala-node containers running" >&2; return 1; }
      resolve_container_name "${scala_ids[0]}"
      ;;
    rust)
      [ "${#rust_ids[@]}" -gt 0 ] || { echo "no rust-node containers running" >&2; return 1; }
      resolve_container_name "${rust_ids[0]}"
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
      resolve_container_name "${all_ids[$idx]}"
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

cmd_interactive() {
  require_docker
  if ! dc ps -q scala-node rust-node 2>/dev/null | grep -q .; then
    echo "cluster is not running — start with: $0 up" >&2
    exit 1
  fi
  echo "Interactive interop cluster manager."
  echo "Commands: list | add scala|rust [N] | remove scala|rust [N] | status | attach scala|rust | logs | quit"
  echo
  while true; do
    read -r -p "interop> " cmd arg1 arg2 || break
    cmd="${cmd:-}"
    case "$cmd" in
      ''|help)
        echo "  list | add scala|rust [N] | remove scala|rust [N] | status | attach scala|rust | logs | quit"
        ;;
      list|ls)
        cmd_list
        ;;
      add)
        [ -n "$arg1" ] || { echo "usage: add <scala|rust> [count]" >&2; continue; }
        cmd_add "$arg1" "${arg2:-1}" || true
        ;;
      remove|rm)
        [ -n "$arg1" ] || { echo "usage: remove <scala|rust> [index]" >&2; continue; }
        cmd_remove "$arg1" "$arg2" || true
        ;;
      status|ps)
        cmd_status
        ;;
      attach)
        [ -n "$arg1" ] || { echo "usage: attach <scala|rust>" >&2; continue; }
        cmd_attach "$arg1"
        ;;
      logs)
        cmd_logs
        ;;
      quit|exit|q)
        break
        ;;
      *)
        echo "unknown command: $cmd (type 'help')" >&2
        ;;
    esac
  done
}

cmd_help() {
  sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-help}" in
  up|start)           shift; cmd_up "$@" ;;
  down|stop)          shift; cmd_down "$@" ;;
  status|ps)          shift; cmd_status "$@" ;;
  list|ls)            shift; cmd_list "$@" ;;
  add)                shift; cmd_add "$@" ;;
  remove|rm)          shift; cmd_remove "$@" ;;
  interactive|repl)   shift; cmd_interactive "$@" ;;
  logs)               shift; cmd_logs "$@" ;;
  attach)             shift; cmd_attach "$@" ;;
  help|-h|--help)     cmd_help ;;
  *)
    echo "unknown command: $1" >&2
    cmd_help
    exit 2
    ;;
esac
