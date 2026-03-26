package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dotandev/hustler/api/proto"
	"github.com/dotandev/hustler/internal/raft"
	protolib "google.golang.org/protobuf/proto"
)

type Task struct {
	ID        string
	Command   string
	Schedule  time.Time
	Completed bool
}

type Worker struct {
	mu      sync.Mutex
	tasks   map[string]*Task
	applyCh chan raft.LogEntry
}

func NewWorker(applyCh chan raft.LogEntry) *Worker {
	return &Worker{
		tasks:   make(map[string]*Task),
		applyCh: applyCh,
	}
}

func (w *Worker) Start(ctx context.Context) {
	go w.runApplyLoop(ctx)
	go w.runExecutionLoop(ctx)
}

func (w *Worker) runApplyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-w.applyCh:
			w.applyEntry(entry)
		}
	}
}

func (w *Worker) applyEntry(entry raft.LogEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var req proto.ScheduleTaskRequest
	if err := protolib.Unmarshal(entry.Command, &req); err != nil {
		log.Printf("Worker: failed to unmarshal task: %v", err)
		return
	}

	if _, exists := w.tasks[req.TaskId]; !exists {
		w.tasks[req.TaskId] = &Task{
			ID:       req.TaskId,
			Command:  req.Command,
			Schedule: time.Unix(req.ScheduleTimeUnix, 0),
		}
		log.Printf("Worker: scheduled task %s for %v", req.TaskId, w.tasks[req.TaskId].Schedule)
	}
}

func (w *Worker) runExecutionLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.executeDueTasks()
		}
	}
}

func (w *Worker) executeDueTasks() {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	for _, task := range w.tasks {
		if !task.Completed && now.After(task.Schedule) {
			w.executeTask(task)
		}
	}
}

func (w *Worker) executeTask(task *Task) {
	log.Printf("Worker: EXECUTING task %s: %s", task.ID, task.Command)
	// In a real system, you'd run the command here (e.g., shell, HTTP call, etc.)
	task.Completed = true
	fmt.Printf("[ALARM] Task %s executed: %s\n", task.ID, task.Command)
}
