package raft

import (
	"encoding/json"
	"os"
)

type PersistentState struct {
	CurrentTerm int64
	VotedFor    string
	Log         []LogEntry
}

func (n *Node) persist() {
	state := PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}
	data, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(n.id+".raft", data, 0644)
	if err != nil {
		panic(err)
	}
}

func (n *Node) readPersist() {
	data, err := os.ReadFile(n.id+".raft")
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		panic(err)
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		panic(err)
	}
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.log = state.Log
}
