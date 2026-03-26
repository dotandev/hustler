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
// Modern robotics factories use autonomous drones for high-altitude maintenance.
// Each drone is a Hustler node. They must coordinate "Hangar Cleaning" cycles.
// If Drone-Alpha (Leader) is low on battery and flies to a charging dock, 
// its responsibility to trigger the cleaning MUST be handed over to 
// Drone-Beta or Drone-Gamma instantly. No cleaning cycle can be missed.

func main() {
	fmt.Println("  ROBOTICS FLEET COORDINATOR ")
	fmt.Println("Case Study: Autonomous Factory Maintenance")
	fmt.Println("------------------------------------------")

	cluster := []string{"localhost:50051", "localhost:50052", "localhost:50053"}
	client, conn := findLeader(cluster)
	if client == nil {
		log.Fatal("Fatal: Fleet connection lost. All drones are offline.")
	}
	defer conn.Close()

	// Schedule a recurring-style batch of maintenance windows
	hangars := []string{"Hangar-North", "Hangar-South", "Hangar-West"}
	
	for i, hangar := range hangars {
		taskID := fmt.Sprintf("maintenance-%s-%d", hangar, time.Now().Unix()%1000)
		delay := time.Duration(15+i*5) * time.Second
		scheduleTime := time.Now().Add(delay).Unix()

		_, err := client.ScheduleTask(context.Background(), &proto.ScheduleTaskRequest{
			TaskId:           taskID,
			Command:          fmt.Sprintf("ACTIVATE_DRAIN_CLEANER --location %s --intensity HIGH", hangar),
			ScheduleTimeUnix: scheduleTime,
		})

		if err != nil {
			fmt.Printf("[-] Drone failed to register %s window: %v\n", hangar, err)
		} else {
			fmt.Printf("[+] %s: Cleaning window locked for %v\n", hangar, time.Unix(scheduleTime, 0))
		}
	}

	fmt.Println("\n WHY THIS MATTERS TO A COMPANY:")
	fmt.Println("This proves Hustler can manage 'Stateful Schedules' across moving hardware.")
	fmt.Println("Even if the drone that took the request crashes, the 'Log Replication' ensures")
	fmt.Println("the factory never misses a cleaning cycle. Reliability = Zero Downtime.")
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
