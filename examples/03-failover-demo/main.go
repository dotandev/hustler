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

func main() {
	fmt.Println("🚀 HUSTLER FAILOVER DEMO 🚀")
	fmt.Println("---------------------------")

	servers := []string{"localhost:50051", "localhost:50052", "localhost:50053"}
	var leaderAddr string

	// Find the current leader
	for {
		for _, addr := range servers {
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			client := proto.NewHustlerClient(conn)
			resp, err := client.GetStatus(context.Background(), &proto.GetStatusRequest{})
			if err == nil && resp.IsLeader {
				leaderAddr = addr
				conn.Close()
				break
			}
			conn.Close()
		}
		if leaderAddr != "" {
			break
		}
		fmt.Println("⏳ Waiting for a leader to be elected...")
		time.Sleep(2 * time.Second)
	}

	fmt.Printf("✅ Found Leader at %s\n", leaderAddr)

	// Schedule a critical task with a 30s delay
	conn, _ := grpc.Dial(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := proto.NewHustlerClient(conn)
	
	taskID := "failover-task-99"
	delay := 30 * time.Second
	scheduleTime := time.Now().Add(delay).Unix()

	resp, _ := client.ScheduleTask(context.Background(), &proto.ScheduleTaskRequest{
		TaskId:           taskID,
		Command:          "echo 'RESILIENCE TEST PASSED'",
		ScheduleTimeUnix: scheduleTime,
	})
	conn.Close()

	if resp.Success {
		fmt.Printf("📝 Scheduled critical task '%s' for %v\n", taskID, time.Unix(scheduleTime, 0))
	} else {
		log.Fatalf("❌ Failed to schedule task: %s", resp.Message)
	}

	fmt.Println("\n🔥 ACTION REQUIRED! 🔥")
	fmt.Printf("Go to the terminal where ./demo.sh is running and KILL the leader (%s).\n", leaderAddr)
	fmt.Println("We will now monitor the cluster to see who takes over...")
	fmt.Println("---------------------------")

	// Monitor transitions
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Printf("\n--- Status at %s ---\n", time.Now().Format("15:04:05"))
		for _, addr := range servers {
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(500*time.Millisecond))
			if err != nil {
				fmt.Printf("[%s] 💀 OFFLINE\n", addr)
				continue
			}
			client := proto.NewHustlerClient(conn)
			status, err := client.GetStatus(context.Background(), &proto.GetStatusRequest{})
			if err != nil {
				fmt.Printf("[%s] ⏳ UNREACHABLE\n", addr)
			} else {
				role := "Follower"
				if status.IsLeader {
					role = "👑 LEADER"
				}
				fmt.Printf("[%s] Role: %s | Term: %d | Applied: %d\n", addr, role, status.Term, status.LastApplied)
			}
			conn.Close()
		}

		if time.Now().Unix() > scheduleTime+5 {
			fmt.Println("\n✅ Time has passed! Check the logs of the remaining nodes to see the alarm ring.")
			fmt.Println("This proves the task survived the crash and was executed by the new leader.")
			return
		}
	}
}
