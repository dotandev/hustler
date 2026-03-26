package raft

import (
	"testing"
	"time"
)

type MockNetwork struct{}

func (m *MockNetwork) RequestVote(targetID string, term int64, candidateID string, lastLogIndex int64, lastLogTerm int64) (int64, bool) {
	return term, true
}

func (m *MockNetwork) AppendEntries(targetID string, term int64, leaderID string, prevLogIndex int64, prevLogTerm int64, entries []LogEntry, leaderCommit int64) (int64, bool) {
	return term, true
}

func TestNewNode(t *testing.T) {
	applyCh := make(chan LogEntry, 100)
	n := NewNode("node1", []string{"node2", "node3"}, &MockNetwork{}, applyCh)

	if n.id != "node1" {
		t.Errorf("expected id node1, got %s", n.id)
	}
	if n.state != Follower {
		t.Errorf("expected state Follower, got %v", n.state)
	}
}

func TestLeaderElection(t *testing.T) {
	applyCh := make(chan LogEntry, 100)
	net := &MockNetwork{}
	n := NewNode("node1", []string{"node2", "node3"}, net, applyCh)

	n.mu.Lock()
	n.startElection()
	n.mu.Unlock()

	// Wait for election to complete (mock network grants votes immediately)
	time.Sleep(100 * time.Millisecond)

	_, isLeader := n.GetState()
	if !isLeader {
		t.Errorf("expected node to become leader")
	}
}

func TestLogComparison(t *testing.T) {
	applyCh := make(chan LogEntry, 100)
	n := NewNode("node1", []string{}, &MockNetwork{}, applyCh)

	n.log = append(n.log, LogEntry{Term: 1, Index: 0})
	n.log = append(n.log, LogEntry{Term: 2, Index: 1})

	// Case 1: Candidate has lower term
	_, granted := n.RequestVote(1, "node2", 1, 2)
	if granted {
		t.Errorf("expected vote rejection for lower term")
	}

	// Case 2: Candidate has same term but shorter log
	_, granted = n.RequestVote(2, "node2", 0, 2)
	if granted {
		t.Errorf("expected vote rejection for shorter log")
	}

	// Case 3: Candidate has higher term
	_, granted = n.RequestVote(3, "node2", 0, 3)
	if !granted {
		t.Errorf("expected vote grant for higher term")
	}
}
