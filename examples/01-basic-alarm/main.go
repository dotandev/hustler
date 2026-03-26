package main

import (
	"context"
	"log"
	"time"

	"github.com/dotandev/hustler/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to the leader node (default localhost:50051)
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := proto.NewHustlerClient(conn)

	// Schedule a "Wake Up" task for 5 seconds from now
	delay := 5 * time.Second
	scheduleTime := time.Now().Add(delay).Unix()

	resp, err := client.ScheduleTask(context.Background(), &proto.ScheduleTaskRequest{
		TaskId:           "alarm-01",
		Command:          "echo 'WAKE UP! It is time to code.'",
		ScheduleTimeUnix: scheduleTime,
	})

	if err != nil {
		log.Fatalf("could not schedule alarm: %v", err)
	}

	if resp.Success {
		log.Printf("SUCCESS: Alarm scheduled for %v", time.Unix(scheduleTime, 0))
	} else {
		log.Printf("FAILED: %s", resp.Message)
	}
}
