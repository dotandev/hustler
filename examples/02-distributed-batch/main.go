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
	// In a real cluster, you might try multiple nodes until you find the leader
	servers := []string{"localhost:50051", "localhost:50052", "localhost:50053"}
	
	var client proto.HustlerClient
	var conn *grpc.ClientConn
	var err error

	for _, addr := range servers {
		conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		
		client = proto.NewHustlerClient(conn)
		// Quick check if this is the leader
		_, err = client.GetStatus(context.Background(), &proto.GetStatusRequest{})
		if err == nil {
			log.Printf("Connected to potential leader at %s", addr)
			break
		}
		conn.Close()
	}

	if client == nil {
		log.Fatal("Could not find any active node in the cluster")
	}
	defer conn.Close()

	// Batch schedule 5 regional cleanup tasks
	regions := []string{"us-east-1", "us-west-2", "eu-central-1", "ap-southeast-1", "sa-east-1"}
	baseDelay := 10 * time.Second

	for i, region := range regions {
		taskID := fmt.Sprintf("cleanup-%s", region)
		scheduleTime := time.Now().Add(baseDelay + time.Duration(i)*time.Second).Unix()

		resp, err := client.ScheduleTask(context.Background(), &proto.ScheduleTaskRequest{
			TaskId:           taskID,
			Command:          fmt.Sprintf("DB_CLEANUP --region %s", region),
			ScheduleTimeUnix: scheduleTime,
		})

		if err != nil || !resp.Success {
			log.Printf("[-] Failed to schedule cleanup for %s: %v", region, resp.Message)
		} else {
			log.Printf("[+] Scheduled cleanup for %s at %v", region, time.Unix(scheduleTime, 0))
		}
	}
}
