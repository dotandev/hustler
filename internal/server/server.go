package server

import (
	"context"

	"github.com/dotandev/hustler/api/proto"
	"github.com/dotandev/hustler/internal/raft"
	protolib "google.golang.org/protobuf/proto"
)

type HustlerServer struct {
	proto.UnimplementedHustlerServer
	node    *raft.Node
	applyCh chan raft.LogEntry
}

func NewHustlerServer(node *raft.Node, applyCh chan raft.LogEntry) *HustlerServer {
	return &HustlerServer{
		node:    node,
		applyCh: applyCh,
	}
}

func (s *HustlerServer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	term, granted := s.node.RequestVote(req.Term, req.CandidateId, req.LastLogIndex, req.LastLogTerm)
	return &proto.RequestVoteResponse{
		Term:        term,
		VoteGranted: granted,
	}, nil
}

func (s *HustlerServer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	entries := make([]raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = raft.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		}
	}

	term, success := s.node.AppendEntries(req.Term, req.LeaderId, req.PrevLogIndex, req.PrevLogTerm, entries, req.LeaderCommit)
	return &proto.AppendEntriesResponse{
		Term:    term,
		Success: success,
	}, nil
}

func (s *HustlerServer) ScheduleTask(ctx context.Context, req *proto.ScheduleTaskRequest) (*proto.ScheduleTaskResponse, error) {
	data, err := protolib.Marshal(req)
	if err != nil {
		return &proto.ScheduleTaskResponse{Success: false, Message: "Internal error"}, nil
	}

	_, _, isLeader := s.node.Propose(data)
	if !isLeader {
		return &proto.ScheduleTaskResponse{Success: false, Message: "Not the leader"}, nil
	}

	return &proto.ScheduleTaskResponse{
		Success: true,
		Message: "Task scheduled successfully",
	}, nil
}

func (s *HustlerServer) GetStatus(ctx context.Context, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	term, isLeader, id, commit, applied := s.node.GetInternalStatus()
	return &proto.GetStatusResponse{
		Term:        term,
		IsLeader:    isLeader,
		NodeId:      id,
		CommitIndex: commit,
		LastApplied: applied,
	}, nil
}
