#!/usr/bin/env bash
# Three-process failover demonstration for KafkaDS.
#
# Starts three broker processes on loopback, writes committed records to three
# partitions, kills the leader of partition 0 with SIGKILL, and checks that:
#   1. every acknowledged record is still readable (from a new leader),
#   2. writes keep committing with one broker gone,
#   3. the killed broker catches up after a restart, with no record stored twice.
#
# Usage: scripts/failover-demo.sh            (builds target/kafkads.jar if missing)
# Needs: bash, java 17+, mvn (only if the jar is missing). Uses ports 9091-9093.
set -euo pipefail

cd "$(dirname "$0")/.."
JAR=target/kafkads.jar
if [[ ! -f "$JAR" ]]; then
  echo "== building $JAR"
  mvn -q -B -DskipTests package
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/kafkads-demo.XXXXXX")
BOOTSTRAP=127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093
declare -a PIDS=(0 0 0 0)

cli()    { java -cp "$JAR" com.kafkads.cli.Cli "$@"; }
status() { cli status "127.0.0.1:909$1" 2>/dev/null; }

start_broker() {
  local id=$1 peers=""
  for other in 1 2 3; do
    [[ $other == "$id" ]] || peers+="${peers:+,}${other}@127.0.0.1:909${other}"
  done
  cat > "$WORK/broker-$id.properties" <<CONF
node.id=$id
host=127.0.0.1
port=909$id
peers=$peers
log.dir=$WORK/data/broker-$id
partition.count=3
replication.factor=3
raft.election.timeout.min.ms=300
raft.election.timeout.max.ms=600
raft.heartbeat.interval.ms=100
CONF
  java -jar "$JAR" "$WORK/broker-$id.properties" >> "$WORK/broker-$id.out" 2>&1 &
  PIDS[$id]=$!
}

cleanup() {
  for id in 1 2 3; do
    [[ ${PIDS[$id]} != 0 ]] && kill -9 "${PIDS[$id]}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

wait_for() { # wait_for <seconds> <description> <command...>
  local seconds=$1 what=$2; shift 2
  for _ in $(seq 1 $((seconds * 5))); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  echo "TIMEOUT waiting for: $what" >&2
  exit 1
}

leader_of() { # prints the id of the broker that reports itself leader of partition $1
  for id in 1 2 3; do
    [[ ${PIDS[$id]} == 0 ]] && continue
    if status "$id" | grep -q "^partition=$1 .* leader=$id "; then echo "$id"; return 0; fi
  done
  return 1
}

offsets_of() { # prints "logEndOffset highWatermark" of partition $2 on broker $1
  status "$1" | sed -n "s/^partition=$2 .*logEndOffset=\([0-9-]*\) highWatermark=\([0-9-]*\)$/\1 \2/p"
}

converged() { # broker $1 has the same log end and high-watermark as the leader, for every partition
  for p in 0 1 2; do
    local l; l=$(leader_of "$p") || return 1
    local mine theirs; mine=$(offsets_of "$1" "$p"); theirs=$(offsets_of "$l" "$p")
    [[ -n "$mine" && "$mine" == "$theirs" ]] || return 1
    [[ "${theirs% *}" == "${theirs#* }" ]] || return 1
  done
}

expect_records() { # expect_records <partition> <count>
  local actual dupes
  actual=$(cli consume "$BOOTSTRAP" "$1" | wc -l | tr -d ' ')
  dupes=$(cli consume "$BOOTSTRAP" "$1" | cut -f2 | sort | uniq -d | wc -l | tr -d ' ')
  if [[ "$actual" != "$2" || "$dupes" != 0 ]]; then
    echo "FAIL partition $1: expected $2 distinct records, got $actual with $dupes duplicated" >&2
    exit 1
  fi
  echo "   partition $1: $actual records, no duplicates"
}

echo "== starting 3 broker processes (data in $WORK)"
for id in 1 2 3; do start_broker "$id"; done
for p in 0 1 2; do wait_for 30 "a leader for partition $p" leader_of "$p"; done
cli metadata "$BOOTSTRAP"

echo "== producing 20 committed records (acks=quorum) to each partition"
for p in 0 1 2; do
  cli produce "$BOOTSTRAP" "$p" quorum $(for i in $(seq 1 20); do printf 'p%s-before-%s ' "$p" "$i"; done) >/dev/null
done
for p in 0 1 2; do expect_records "$p" 20; done

VICTIM=$(leader_of 0)
echo "== kill -9 broker $VICTIM (pid ${PIDS[$VICTIM]}), the leader of partition 0"
kill -9 "${PIDS[$VICTIM]}"
wait "${PIDS[$VICTIM]}" 2>/dev/null || true
PIDS[$VICTIM]=0

for p in 0 1 2; do wait_for 30 "a new leader for partition $p" leader_of "$p"; done
echo "== new leaders: p0=$(leader_of 0) p1=$(leader_of 1) p2=$(leader_of 2); reading everything back"
for p in 0 1 2; do expect_records "$p" 20; done

echo "== producing 10 more committed records per partition with broker $VICTIM down"
for p in 0 1 2; do
  cli produce "$BOOTSTRAP" "$p" quorum $(for i in $(seq 1 10); do printf 'p%s-degraded-%s ' "$p" "$i"; done) >/dev/null
done
for p in 0 1 2; do expect_records "$p" 30; done

echo "== acks=all must be refused while a replica is down"
if cli produce "$BOOTSTRAP" 0 all should-not-be-acknowledged >/dev/null 2>"$WORK/all.err"; then
  echo "FAIL: acks=all succeeded with a replica down" >&2; exit 1
fi
echo "   refused: $(cat "$WORK/all.err")"

echo "== restarting broker $VICTIM on its old data directory"
start_broker "$VICTIM"
wait_for 30 "broker $VICTIM to answer" status "$VICTIM"
wait_for 30 "broker $VICTIM to converge" converged "$VICTIM"
status "$VICTIM"
# The refused acks=all record was still committed by the majority (documented behaviour).
expect_records 0 31; expect_records 1 30; expect_records 2 30

echo "== acks=all succeeds again now that every replica is back"
cli produce "$BOOTSTRAP" 0 all all-replicas-have-this
expect_records 0 32

echo "PASS"
