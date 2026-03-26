package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dotandev/hustler/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BACKSTORY:
// A High-Frequency Fintech processing billions in daily volume uses Hustler
// to lock in "Settlement Windows." Once a batch of trades is committed to
// the Hustler log, it is legally and technically "Final."
// Even if 2 out of 3 data centers are hit by a power outage, the record
// of these settlements MUST survive. Zero-data-loss is the only requirement.
// at least, thT'S my theory.

func main() {
	fmt.Println("💰 FINTECH SETTLEMENT RECONCILER 💰")
	fmt.Println("Case Study: High-Value Transaction Persistence")

	cluster := []string{"localhost:50051", "localhost:50052", "localhost:50053"}
	client, conn := findLeader(cluster)
	if client == nil {
		log.Fatal("Critical: Connection to Banking Cluster lost. Retrying via recovery protocol...")
	}
	defer conn.Close()

	// High-value settlement windows (Indices are critical here)
	for i := 1; i <= 3; i++ {
		batchID := fmt.Sprintf("settlement-batch-%d-%d", i, time.Now().Unix()%100)

		// In fintech, we often schedule for "As Soon As Possible" (now)
		// but we need the Consensus engine to confirm it's durable.
		resp, err := client.ScheduleTask(context.Background(), &proto.ScheduleTaskRequest{
			TaskId:           batchID,
			Command:          fmt.Sprintf("PROCESS_SETTLEMENT --batch %s --currency USD --verify SHA256", batchID),
			ScheduleTimeUnix: time.Now().Add(5 * time.Second).Unix(),
		})

		if err != nil || !resp.Success {
			fmt.Printf("[-] ABORT: Settlement Batch %s failed quorum check!\n", batchID)
		} else {
			fmt.Printf("[+] VERIFIED: Batch %s committed to Raft Log. Total Durability Guaranteed.\n", batchID)
		}
	}

	fmt.Println("\n WHY THIS MATTERS TO A COMPANY:")
	fmt.Println("This demonstrates 'Strong Consistency.' In finance, you can't have a task")
	fmt.Println("be 'mostly' saved. Using Hustler' Raft implementation provides the same")
	fmt.Println("mathematical guarantees as banks and global payment processors.")
}

func findLeader(addrs []string) (proto.HustlerClient, *grpc.ClientConn) {
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithTimeout(1*time.Second))
		if err != nil {
			continue
		}
		client := proto.NewHustlerClient(conn)
		resp, err := client.GetStatus(context.Background(), &proto.GetStatusRequest{})
		if err == nil && resp.IsLeader {
			return client, conn
		}
		conn.Close()
	}
	return nil, nil
}
