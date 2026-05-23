/**
 * Raft Consensus Node
 * Implements: leader election (RequestVote), log replication (AppendEntries),
 * majority quorum, term-based ordering, and commit index advancement.
 *
 * Wire protocol: plain HTTP/JSON – no external deps.
 * Timing follows the Raft paper (§5.2): randomized election timeouts,
 * fixed heartbeat interval well below the timeout floor.
 */

'use strict';

const http = require('http');

// ─── tunables ────────────────────────────────────────────────────────────────
const ELECTION_TIMEOUT_MIN = 150;   // ms
const ELECTION_TIMEOUT_MAX = 300;   // ms
const HEARTBEAT_INTERVAL   = 50;    // ms  (must be << ELECTION_TIMEOUT_MIN)
const RPC_TIMEOUT          = 80;    // ms  per outbound call

// ─── helpers ─────────────────────────────────────────────────────────────────

function randomTimeout() {
  return ELECTION_TIMEOUT_MIN +
         Math.floor(Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN));
}

/** Fire-and-forget HTTP POST; resolves with parsed body or rejects on error/timeout */
function rpc(host, port, path, body, timeoutMs = RPC_TIMEOUT) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = http.request(
      { host, port, path, method: 'POST',
        headers: { 'Content-Type': 'application/json',
                   'Content-Length': Buffer.byteLength(payload) } },
      (res) => {
        let raw = '';
        res.on('data', d => raw += d);
        res.on('end', () => {
          try { resolve(JSON.parse(raw)); }
          catch { reject(new Error('bad json')); }
        });
      }
    );
    req.setTimeout(timeoutMs, () => { req.destroy(); reject(new Error('timeout')); });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// ─── Raft state machine ───────────────────────────────────────────────────────

class RaftNode {
  constructor(id, port, peers) {
    this.id   = id;      // string  e.g. "node1"
    this.port = port;    // number
    this.peers = peers;  // [{ id, host, port }, ...]

    // persistent state (would be on disk in production)
    this.currentTerm  = 0;
    this.votedFor     = null;
    this.log          = [];   // [{term, index, command}]

    // volatile state
    this.commitIndex  = -1;
    this.lastApplied  = -1;
    this.state        = 'follower';  // follower | candidate | leader
    this.leaderId     = null;

    // leader volatile state
    this.nextIndex    = {};   // peerId -> next log index to send
    this.matchIndex   = {};   // peerId -> highest replicated index

    // the replicated state machine: a single JSON object
    this.stateMachine = {};

    // timers
    this._electionTimer   = null;
    this._heartbeatTimer  = null;

    // event log for the dashboard
    this.events = [];
  }

  // ── logging ──────────────────────────────────────────────────────────────

  _log(msg) {
    const entry = `[${this.id}|T${this.currentTerm}|${this.state.toUpperCase().slice(0,1)}] ${msg}`;
    console.log(entry);
    this.events.unshift({ ts: Date.now(), msg: entry });
    if (this.events.length > 100) this.events.length = 100;
  }

  // ── timer management ─────────────────────────────────────────────────────

  _resetElectionTimer() {
    clearTimeout(this._electionTimer);
    this._electionTimer = setTimeout(() => this._startElection(), randomTimeout());
  }

  _stopElectionTimer() {
    clearTimeout(this._electionTimer);
    this._electionTimer = null;
  }

  _startHeartbeatTimer() {
    clearInterval(this._heartbeatTimer);
    this._heartbeatTimer = setInterval(() => this._sendHeartbeats(), HEARTBEAT_INTERVAL);
  }

  _stopHeartbeatTimer() {
    clearInterval(this._heartbeatTimer);
    this._heartbeatTimer = null;
  }

  // ── state transitions ────────────────────────────────────────────────────

  _becomeFollower(term) {
    const prev = this.state;
    this.state = 'follower';
    this.currentTerm = term;
    this.votedFor    = null;
    this.leaderId    = null;
    this._stopHeartbeatTimer();
    this._resetElectionTimer();
    if (prev !== 'follower') this._log(`became FOLLOWER (term ${term})`);
  }

  _becomeLeader() {
    if (this.state !== 'candidate') return;
    this.state    = 'leader';
    this.leaderId = this.id;
    this._stopElectionTimer();
    // initialize nextIndex / matchIndex per Raft §5.3
    for (const p of this.peers) {
      this.nextIndex[p.id]  = this.log.length;
      this.matchIndex[p.id] = -1;
    }
    this._log(`became LEADER`);
    this._startHeartbeatTimer();
    // immediately send empty AppendEntries (leadership assertion)
    this._sendHeartbeats();
  }

  // ── leader election ──────────────────────────────────────────────────────

  _startElection() {
    this.state       = 'candidate';
    this.currentTerm += 1;
    this.votedFor    = this.id;
    this.leaderId    = null;
    this._log(`starting election for term ${this.currentTerm}`);
    this._resetElectionTimer();

    let votes = 1;  // vote for self
    const majority = Math.floor((this.peers.length + 1) / 2) + 1;

    const lastLogIndex = this.log.length - 1;
    const lastLogTerm  = lastLogIndex >= 0 ? this.log[lastLogIndex].term : 0;

    const voteReq = {
      term: this.currentTerm,
      candidateId: this.id,
      lastLogIndex,
      lastLogTerm,
    };

    for (const peer of this.peers) {
      rpc(peer.host, peer.port, '/raft/request-vote', voteReq)
        .then(resp => {
          if (this.state !== 'candidate') return;
          if (resp.term > this.currentTerm) {
            this._becomeFollower(resp.term);
            return;
          }
          if (resp.voteGranted) {
            votes++;
            this._log(`got vote from ${peer.id} (${votes}/${majority})`);
            if (votes >= majority) this._becomeLeader();
          }
        })
        .catch(() => { /* peer unreachable */ });
    }
  }

  // ── RPC handlers ─────────────────────────────────────────────────────────

  handleRequestVote(req) {
    // §5.2 / §5.4: grant vote only if term is current and log is at least as up-to-date
    if (req.term > this.currentTerm) this._becomeFollower(req.term);

    const lastLogIndex = this.log.length - 1;
    const lastLogTerm  = lastLogIndex >= 0 ? this.log[lastLogIndex].term : 0;
    const logOk = req.lastLogTerm > lastLogTerm ||
                  (req.lastLogTerm === lastLogTerm && req.lastLogIndex >= lastLogIndex);

    const granted = req.term === this.currentTerm &&
                    logOk &&
                    (this.votedFor === null || this.votedFor === req.candidateId);

    if (granted) {
      this.votedFor = req.candidateId;
      this._resetElectionTimer();
      this._log(`voted for ${req.candidateId}`);
    }

    return { term: this.currentTerm, voteGranted: granted };
  }

  handleAppendEntries(req) {
    // §5.1: if we see a higher term, step down
    if (req.term > this.currentTerm) this._becomeFollower(req.term);

    if (req.term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }

    // valid leader contact – reset election timer
    this._resetElectionTimer();
    this.leaderId = req.leaderId;
    if (this.state !== 'follower') this._becomeFollower(req.term);

    // §5.3 log consistency check
    if (req.prevLogIndex >= 0) {
      const prev = this.log[req.prevLogIndex];
      if (!prev || prev.term !== req.prevLogTerm) {
        return { term: this.currentTerm, success: false,
                 conflictIndex: Math.min(req.prevLogIndex, this.log.length) };
      }
    }

    // append any new entries (deleting conflicting ones first)
    if (req.entries && req.entries.length > 0) {
      this.log = this.log.slice(0, req.prevLogIndex + 1);
      this.log.push(...req.entries);
    }

    // advance commit index
    if (req.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(req.leaderCommit, this.log.length - 1);
      this._applyCommitted();
    }

    return { term: this.currentTerm, success: true,
             matchIndex: this.log.length - 1 };
  }

  // ── log replication ──────────────────────────────────────────────────────

  _sendHeartbeats() {
    for (const peer of this.peers) {
      this._replicateToPeer(peer);
    }
  }

  _replicateToPeer(peer) {
    const ni        = this.nextIndex[peer.id] ?? this.log.length;
    const prevIndex = ni - 1;
    const prevTerm  = prevIndex >= 0 && this.log[prevIndex]
                      ? this.log[prevIndex].term : 0;
    const entries   = this.log.slice(ni);

    const payload = {
      term:         this.currentTerm,
      leaderId:     this.id,
      prevLogIndex: prevIndex,
      prevLogTerm:  prevTerm,
      entries,
      leaderCommit: this.commitIndex,
    };

    rpc(peer.host, peer.port, '/raft/append-entries', payload)
      .then(resp => {
        if (!resp) return;
        if (resp.term > this.currentTerm) {
          this._becomeFollower(resp.term);
          return;
        }
        if (this.state !== 'leader') return;

        if (resp.success) {
          this.matchIndex[peer.id] = resp.matchIndex ?? (ni + entries.length - 1);
          this.nextIndex[peer.id]  = this.matchIndex[peer.id] + 1;
          this._advanceCommitIndex();
        } else {
          // back off by one (or use conflictIndex hint)
          this.nextIndex[peer.id] = Math.max(0,
            (resp.conflictIndex ?? this.nextIndex[peer.id]) - 1);
        }
      })
      .catch(() => { /* unreachable peers are silently ignored */ });
  }

  // ── commit index / state machine ─────────────────────────────────────────

  /**
   * §5.3 + §5.4: leader advances commitIndex when a log entry at N
   * has been replicated on a majority AND entry.term === currentTerm
   */
  _advanceCommitIndex() {
    const majority = Math.floor((this.peers.length + 1) / 2) + 1;
    for (let n = this.log.length - 1; n > this.commitIndex; n--) {
      if (this.log[n].term !== this.currentTerm) continue;
      const count = 1 + this.peers.filter(p => (this.matchIndex[p.id] ?? -1) >= n).length;
      if (count >= majority) {
        this.commitIndex = n;
        this._log(`committed up to index ${n}`);
        this._applyCommitted();
        break;
      }
    }
  }

  _applyCommitted() {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      const entry = this.log[this.lastApplied];
      this._applyCommand(entry.command);
    }
  }

  _applyCommand(cmd) {
    if (!cmd) return;
    if (cmd.op === 'set') {
      this._setNested(this.stateMachine, cmd.path, cmd.value);
    } else if (cmd.op === 'delete') {
      this._deleteNested(this.stateMachine, cmd.path);
    }
  }

  _setNested(obj, path, value) {
    const parts = path.split('.');
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      if (cur[parts[i]] === undefined || typeof cur[parts[i]] !== 'object') {
        cur[parts[i]] = {};
      }
      cur = cur[parts[i]];
    }
    cur[parts[parts.length - 1]] = value;
  }

  _deleteNested(obj, path) {
    const parts = path.split('.');
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!cur[parts[i]]) return;
      cur = cur[parts[i]];
    }
    delete cur[parts[parts.length - 1]];
  }

  // ── client API ───────────────────────────────────────────────────────────

  /**
   * Append a command to the log (only valid on leader).
   * Returns { success, leaderId, index } after log append;
   * commit happens asynchronously via replication.
   */
  propose(command) {
    if (this.state !== 'leader') {
      return { success: false, leaderId: this.leaderId, reason: 'not leader' };
    }
    const entry = { term: this.currentTerm, index: this.log.length, command };
    this.log.push(entry);
    this._log(`proposed ${JSON.stringify(command)} at index ${entry.index}`);
    // immediately try to replicate
    for (const p of this.peers) this._replicateToPeer(p);
    return { success: true, index: entry.index };
  }

  // ── startup ──────────────────────────────────────────────────────────────

  start() {
    this._log('starting');
    this._resetElectionTimer();
    this._startHttpServer();
  }

  // ── HTTP server ──────────────────────────────────────────────────────────

  _startHttpServer() {
    const server = http.createServer((req, res) => {
      let body = '';
      req.on('data', d => body += d);
      req.on('end', () => {
        let result;
        try {
          const data = body ? JSON.parse(body) : {};
          result = this._route(req.method, req.url, data);
        } catch (e) {
          result = { error: e.message };
        }
        const out = JSON.stringify(result);
        res.writeHead(200, { 'Content-Type': 'application/json',
                             'Access-Control-Allow-Origin': '*' });
        res.end(out);
      });
    });
    server.listen(this.port, () => this._log(`listening on :${this.port}`));
  }

  _route(method, url, body) {
    if (url === '/raft/request-vote'  && method === 'POST') return this.handleRequestVote(body);
    if (url === '/raft/append-entries'&& method === 'POST') return this.handleAppendEntries(body);
    if (url === '/api/write'          && method === 'POST') return this.propose(body.command);
    if (url === '/api/read') {
      return {
        state: this.stateMachine,
        meta: {
          id: this.id,
          role: this.state,
          term: this.currentTerm,
          leader: this.leaderId,
          logLength: this.log.length,
          commitIndex: this.commitIndex,
        }
      };
    }
    if (url === '/api/status') {
      return {
        id: this.id,
        role: this.state,
        term: this.currentTerm,
        leader: this.leaderId,
        logLength: this.log.length,
        commitIndex: this.commitIndex,
        lastApplied: this.lastApplied,
        peers: this.peers.length,
        events: this.events.slice(0, 20),
      };
    }
    if (url === '/api/crash' && method === 'POST') {
      this._log('SIMULATED CRASH – stopping timers');
      this._stopElectionTimer();
      this._stopHeartbeatTimer();
      this.state = 'crashed';
      return { crashed: true };
    }
    if (url === '/api/recover' && method === 'POST') {
      this._log('RECOVERING');
      this._becomeFollower(this.currentTerm);
      return { recovered: true };
    }
    return { error: 'not found' };
  }
}

// ─── bootstrap ───────────────────────────────────────────────────────────────

const [,, idArg, portArg, peersArg] = process.argv;
if (!idArg || !portArg || !peersArg) {
  console.error('usage: node raft.js <id> <port> <peer1:host:port,...>');
  process.exit(1);
}

const peers = peersArg.split(',').map(s => {
  const [id, host, port] = s.split(':');
  return { id, host, port: Number(port) };
});

const node = new RaftNode(idArg, Number(portArg), peers);
node.start();
