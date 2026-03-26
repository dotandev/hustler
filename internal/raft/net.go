package raft

import (
	"context"
	"log"
	"time"

	"github.com/dotandev/hustler/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCNetwork struct {
	addrMap map[string]string // nodeID -> addr (e.g. "node1" -> "localhost:50051")
}

func NewGRPCNetwork(addrMap map[string]string) *GRPCNetwork {
	return &GRPCNetwork{
		addrMap: addrMap,
	}
}

func (g *GRPCNetwork) RequestVote(targetID string, term int64, candidateID string, lastLogIndex int64, lastLogTerm int64) (int64, bool) {
	addr, ok := g.addrMap[targetID]
	if !ok {
		log.Printf("GRPCNetwork: unknown target %s", targetID)
		return 0, false
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, false
	}
	defer conn.Close()

	client := proto.NewHustlerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	resp, err := client.RequestVote(ctx, &proto.RequestVoteRequest{
		Term:         term,
		CandidateId:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	})
	if err != nil {
		return 0, false
	}

	return resp.Term, resp.VoteGranted
}

func (g *GRPCNetwork) AppendEntries(targetID string, term int64, leaderID string, prevLogIndex int64, prevLogTerm int64, entries []LogEntry, leaderCommit int64) (int64, bool) {
	addr, ok := g.addrMap[targetID]
	if !ok {
		return 0, false
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, false
	}
	defer conn.Close()

	client := proto.NewHustlerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	protoEntries := make([]*proto.LogEntry, len(entries))
	for i, e := range entries {
		protoEntries[i] = &proto.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		}
	}

	resp, err := client.AppendEntries(ctx, &proto.AppendEntriesRequest{
		Term:         term,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      protoEntries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		return 0, false
	}

	return resp.Term, resp.Success
}
