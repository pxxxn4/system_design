#!/usr/bin/env bash
# start-cluster.sh  –  launch five Raft nodes in the background

set -euo pipefail

PEER_SPEC="node1:127.0.0.1:3001,node2:127.0.0.1:3002,node3:127.0.0.1:3003,node4:127.0.0.1:3004,node5:127.0.0.1:3005"

kill_cluster() {
  echo "Stopping cluster..."
  pkill -f "node raft.js" 2>/dev/null || true
}
trap kill_cluster EXIT

mkdir -p logs

for i in 1 2 3 4 5; do
  OTHERS=""
  for j in 1 2 3 4 5; do
    [ "$i" -eq "$j" ] && continue
    [ -n "$OTHERS" ] && OTHERS="${OTHERS},"
    OTHERS="${OTHERS}node${j}:127.0.0.1:300${j}"
  done
  node node/raft.js "node${i}" "300${i}" "$OTHERS" > "logs/node${i}.log" 2>&1 &
  echo "Started node${i} on :300${i}"
done

echo ""
echo "Cluster up. Dashboard: open client/index.html"
echo "Press Ctrl-C to stop."
wait
