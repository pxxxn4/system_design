# Raft Cluster — 5-node consensus demo

A from-scratch implementation of the **Raft consensus algorithm** in Node.js (zero dependencies).

## What's implemented

| Feature | Detail |
|---|---|
| Leader election | Randomised election timeouts (150–300ms), term-based voting, majority quorum |
| Log replication | AppendEntries RPC with prevLogIndex/prevLogTerm consistency check |
| Heartbeats | Leader sends empty AppendEntries every 50ms to prevent re-elections |
| Commit advancement | Leader advances commitIndex once a majority has replicated the entry (§5.3 / §5.4) |
| State machine | Nested JSON object; commands: `set <dotted.path> <value>`, `delete <dotted.path>` |
| Fault tolerance | System stays up as long as ≥3 of 5 nodes are alive (majority quorum) |
| Crash / recovery | Simulated via HTTP; node stops timers, then re-joins as follower on recovery |

## Files

```
raft-cluster/
├── node/
│   └── raft.js          ← core Raft node (election + replication + HTTP server)
├── client/
│   └── index.html       ← live dashboard (topology, node cards, write form, event log)
├── start-cluster.sh     ← starts all 5 nodes on ports 3001-3005
└── README.md
```

## Quick start

```bash
# 1. Make sure Node.js ≥ 18 is installed (no npm install needed)
node --version

# 2. Start the cluster
chmod +x start-cluster.sh
./start-cluster.sh

# 3. Open the dashboard
open client/index.html    # macOS
# or just double-click index.html in your file manager
```

## Manual node startup

Each node needs its own ID, port, and the peer list:

```bash
# In five separate terminals:
node node/raft.js node1 3001 node2:127.0.0.1:3002,node3:127.0.0.1:3003,node4:127.0.0.1:3004,node5:127.0.0.1:3005
node node/raft.js node2 3002 node1:127.0.0.1:3001,node3:127.0.0.1:3003,node4:127.0.0.1:3004,node5:127.0.0.1:3005
node node/raft.js node3 3003 node1:127.0.0.1:3001,node2:127.0.0.1:3002,node4:127.0.0.1:3004,node5:127.0.0.1:3005
node node/raft.js node4 3004 node1:127.0.0.1:3001,node2:127.0.0.1:3002,node3:127.0.0.1:3003,node5:127.0.0.1:3005
node node/raft.js node5 3005 node1:127.0.0.1:3001,node2:127.0.0.1:3002,node3:127.0.0.1:3003,node4:127.0.0.1:3004
```

## HTTP API

Every node exposes:

| Method | Path | Description |
|---|---|---|
| GET | `/api/status` | Node role, term, log length, commit index, recent events |
| GET | `/api/read` | Shared state machine JSON + metadata |
| POST | `/api/write` | Propose a command (only succeeds on the leader) |
| POST | `/api/crash` | Simulate a crash (stops timers) |
| POST | `/api/recover` | Recover as follower |
| POST | `/raft/request-vote` | Internal Raft RPC |
| POST | `/raft/append-entries` | Internal Raft RPC |

### Write example

```bash
curl -X POST http://127.0.0.1:3001/api/write \
     -H 'Content-Type: application/json' \
     -d '{"command":{"op":"set","path":"user.score","value":42}}'
# If node1 is not the leader, returns: {"success":false,"leaderId":"node3","reason":"not leader"}
```

## Key Raft implementation notes

**Election timeout**: each follower picks a random timeout in [150, 300]ms. If no heartbeat arrives within that window, it increments its term, transitions to candidate, votes for itself, and broadcasts RequestVote RPCs.

**Vote granting** (§5.4 safety): a node grants a vote only if:
- the candidate's term ≥ its own current term, AND
- it hasn't voted for anyone else this term, AND
- the candidate's log is at least as up-to-date (by term, then length)

**Log replication**: the leader maintains `nextIndex[peer]` (next entry to send) and `matchIndex[peer]` (highest confirmed). On rejection it backs off `nextIndex` by one; on success it advances both indices and checks whether a new entry can be committed.

**Commit rule** (§5.4): the leader only commits entries from the *current term*. This prevents stale entries from older terms from being incorrectly applied.

**Timing constants** (in `raft.js`):
```
ELECTION_TIMEOUT_MIN = 150 ms
ELECTION_TIMEOUT_MAX = 300 ms
HEARTBEAT_INTERVAL   =  50 ms   (must be << ELECTION_TIMEOUT_MIN)
RPC_TIMEOUT          =  80 ms
```
