#!/bin/bash

# 1. Build the project
echo "Building Hustler..."
go build -o hustler ./cmd/hustler
go build -o hustler-cli ./cmd/hustler-cli

# 2. Start 3 nodes in the background
echo "Starting 3-node cluster..."
./hustler -id node1 -port 50051 -peers node2:localhost:50052,node3:localhost:50053 > node1.log 2>&1 &
PID1=$!
./hustler -id node2 -port 50052 -peers node1:localhost:50051,node3:localhost:50053 > node2.log 2>&1 &
PID2=$!
./hustler -id node3 -port 50053 -peers node1:localhost:50051,node2:localhost:50052 > node3.log 2>&1 &
PID3=$!

echo "Nodes started (PIDs: $PID1, $PID2, $PID3). Waiting 5s for leader election..."
sleep 5

# 3. Check status
echo "Checking cluster status..."
./hustler-cli -server localhost:50051 -status
./hustler-cli -server localhost:50052 -status
./hustler-cli -server localhost:50053 -status

echo ""
echo "--- DEMO INSTRUCTIONS ---"
echo "1. Schedule a task: ./hustler-cli -server localhost:50051 -id task-1 -cmd 'echo hello' -delay 5s"
echo "2. Watch logs: tail -f node1.log node2.log node3.log"
echo "3. Kill a node: kill $PID1"
echo "4. Check new leader after 5s: ./hustler-cli -server localhost:50052 -status"
echo "--------------------------"

# Function to cleanup on exit
cleanup() {
    echo "Stopping nodes..."
    kill $PID1 $PID2 $PID3
    rm hustler hustler-cli
}

trap cleanup EXIT

# Keep script running to see output
wait
