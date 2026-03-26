# Hustler: The Closer's Contributor Guide (ELI5)

Welcome to the team! Here is how our "Elite Squad" of Closers works, explained so any engineer can jump in.

## 1. The Core Brain (`raft.go`)

### The Hub (`Node` struct)
Think of this as the **Hustler's Briefcase**. It stores their connections, their reputation, and their current "Grind."
- `currentTerm`: The "Season of the Hustle." What game number we are currently on.
- `votedFor`: Who we backed as the "Top Hustler" (The Closer) this season.
- `log`: The **Master Ledger** of deals.

### The Grind States (`State`)
- **Follower**: Learning from the pro, keeping the records straight.
- **Candidate**: "Look, I'm ready to close the big deals! Vote for me!"
- **Leader**: The undisputed Closer. You handle the clients; the team follows your lead.

### Networking
- `RequestVote`: A hustler asking the crew, "Do you trust me to lead this season?"
- `AppendEntries`: The Closer sharing new deals with the crew, or just checking in to say "I'm still at the top!" (The Status Check).

### The "Next Man Up" Timer (`ElectionTimer`)
If the crew hasn't heard from the Closer in a while, they don't sit around. Someone's timer goes *DING!* and they step up to take over the market.

---

## 2. Staying Smart after a Rest (`persistence.go`)

### The Midnight Grind (`persist` & `readPersist`)
Imagine a hustler finally catching some shut-eye. Before they do, they write their **Master Ledger** and the current **Season Number** on a secure drive (`.raft` file). When they wake up, they check the drive so they can pick up right where they left off. No ground lost.

---

## 3. The Client Hotline (`server.go`)

### Talking to the Market (`HustlerServer`)
This is the hustler's **Hotline**. It's how clients submit new deals (`ScheduleTask`). It's also how the crew communicates across the world.

---

## 4. The Training Ground (`raft_test.go`)

### Pressure Testing (`TestLogReplication`)
This is where we simulate intense market conditions. We throw new deals at a mock crew and make sure every single member has an identical ledger. If the record is perfect, we win.

---

## How YOU can scale the hustle:
1. **Smarter Deal Tracking**: Help the crew identify which deals are closed and buried!
2. **High-Speed Comms**: Make the "Hotline" (gRPC) even faster. Seconds are dollars.
3. **Audit Trail**: Ensure every hustler double-checks the ledger for errors before executing.

Ready to close some deals? 
