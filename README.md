# Hustler Raft Engine: The Ultimate Distributed Closer

Hustler is a high-performance distributed task scheduler built on a custom, battle-tested implementation of the Raft consensus algorithm. It's designed for mission-critical workloads where failure is not an option and every millisecond counts. This document provides a deep dive into how the Hustler "Closer" engine operates in `internal/raft/raft.go`.

---

## 1. Recruitment & Identity

### `NewNode(id string, peers []string, net Network)`
- **Purpose**: Recruits a new hustler into the crew.
- **Why**: Every member needs a unique reputation and a contact list of peers to form a dominant cluster. This function sets up the foundational grind (starting as a `Follower`) and connects the hustler to the communications network.

### `GetState() (int64, bool)`
- **Purpose**: Returns the hustler's current "Season" (`Term`) and whether they are currently "The Closer" (`Leader`).
- **Why**: External systems (like the gRPC server) need to know if they should route client deals to this hustler or redirect them to the top Closer.

---

## 2. Peer-to-Peer RPC Handlers: The Daily Grind

### `RequestVote(term, candidateID, ...)`
- **Purpose**: Handles a request for support from a `Candidate` during an election.
- **Why**: This is how the crew reaches consensus on who leads. It ensures that only one Closer exists per season and that the leader has a sufficiently up-to-date ledger to keep the money safe.

### `AppendEntries(term, leaderID, entries, ...)`
- **Purpose**: Processes a status check or deal replication request from The Closer.
- **Why**: This function accomplishes three things: 
    1. It tells followers the Closer is still active (resetting their takeover timers). 
    2. It replicates deals from the Closer's master ledger to the subordinate ledgers. 
    3. It ensures ledger consistency by rolling back conflicting deals.

---

## 3. Election Mechanics: Taking Over the Market

### `resetElectionTimer()`
- **Purpose**: Resets the randomized countdown for the next takeover attempt.
- **Why**: Jitter is critical to prevent multiple hustlers from trying to take over at the exact same time, which would lead to "split votes" and market stagnation.

### `runElectionLoop()`
- **Purpose**: Background monitoring that waits for the Closer to go silent.
- **Why**: In a high-stakes environment, we need to detect weakness immediately. If no word arrives from the Closer, this loop triggers a takeover to keep the business running.

### `startElection()`
- **Purpose**: Transitions the hustler to a `Candidate`, increments the season, and calls for votes from the crew.
- **Why**: This is the proactive step to recover from a leadership void. It initiates the voting process defined by the Raft safety rules.

---

## 4. Total Market Dominance & Replication

### `transitionToLeader()`
- **Purpose**: Sets up the hustler to act as the crew's undisputed Closer.
- **Why**: Closers need to track where every subordinate is in the ledger synchronization process (`nextIndex`) and confirms what deals they have verified (`matchIndex`).

### `replicateLogs()`
- **Purpose**: Broadcasts the latest deals to the entire crew.
- **Why**: Reaching a majority (quorum) is how Hustler guarantees that once a deal is "inked" (committed), it can never be lost even if some members drop off.

### `updateCommitIndex()`
- **Purpose**: Identifies the highest ledger entry successfully replicated to a majority of the crew.
- **Why**: A deal is only safe to execute once a majority has it. This function calculates that threshold based on the progress of all peers.

---

## 5. Client Interaction: Closing the Deal

### `Propose(command []byte)`
- **Purpose**: Accepts a new deal from the client and adds it to the master ledger.
- **Why**: This is the entry point for all value in the system. It initiates the cycle of replication that eventually makes the deal durable across the globe.

---

## 6. Communication Networking

### `sendRequestVote` & `sendAppendEntries`
- **Purpose**: Wrapper functions that call the `Network` interface to handle the comms.
- **Why**: They abstract away the lower-level networking code (like gRPC) so that the core logic remains fast, lean, and high-performance.

---

## 📚 Examples: Seeing the Hustle in Action

Hustler is designed for everything from simple tasking to mission-critical distributed scheduling. Check out the [examples](./examples) folder:

| Example | Complexity | Description |
| :--- | :--- | :--- |
| [**01-Basic-Alarm**](./examples/01-basic-alarm/main.go) | 🟢 Simple | A single-node client scheduling a "Wake Up" message. |
| [**02-Distributed-Batch**](./examples/02-distributed-batch/main.go) | 🟡 Medium | Scheduling multiple regional deals simultaneously with leader-discovery. |
| [**03-Failover-Demo**](./examples/03-failover-demo/main.go) | 🟠 Advanced | A tour of how Hustler survives when the top Closer vanishes. |
| [**04-Maintenance-Drone**](./examples/04-maintenance-drone/main.go) | 🔴 Professional | Coordinating high-availability maintenance for robotics fleets. |
| [**05-Settlement-Reconciler**](./examples/05-settlement-reconciler/main.go) | 🔴 Professional | High-durability settlement windows for fintech and banking. |

### How to run an example
1. Start the crew: `./demo.sh`
2. Run the example in a new tab: `go run examples/01-basic-alarm/main.go`
