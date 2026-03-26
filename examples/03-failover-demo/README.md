# Example 03: Failover & Resilience

This example is a guided demonstration of Hustler' distributed core: **How it survives when things go wrong.**

## The Scenario
1. We have a 3-node cluster.
2. We schedule a "Mission Critical" task with a 20-second delay.
3. We **KILL** the Leader node while the task is still pending.
4. We observe a new leader being elected.
5. We verify the task **STILL EXECUTES** on time.

## Steps to Reproduce

### 1. Start the cluster
Run the main demo script to get 3 nodes running:
```bash
./demo.sh
```

### 2. Schedule a delayed task
Submission must go to the current leader (check `./hustler-cli -status`):
```bash
./hustler-cli -server localhost:50051 -id critical-job -cmd "REBOOT_SATELLITE" -delay 20s
```

### 3. Kill the Leader
Find the PID of the leader node and kill it:
```bash
# If node1 is leader
kill <PID_OF_NODE1>
```

### 4. Verify Survival
Wait for the 20 seconds to pass. Even though the original boss is dead, one of the followers (node2 or node3) will have taken over as Leader.

Check the logs of the remaining nodes:
```bash
tail -f node2.log node3.log
```

You will see the `[ALARM]` trigger successfully! This proves that the task was **replicated** safely before the crash.
