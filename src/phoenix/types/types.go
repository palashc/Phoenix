package types

import "time"

// Task simulates Phoenix tasks with a sleep time between 0 and 1 seconds

type TaskID string

type Task struct {
	JobId string
	Id    string
	T     float64
}

type TaskRequest struct {
	JobId 	 	string
	WorkerAddr	string
}

type WorkerTaskCompleteMsg struct {
	TaskID     string
	WorkerAddr string
}

type TaskRecord struct {
	Task Task

	AssignedWorker int
	Finished       bool
}

// Job is an array of Tasks
type Job struct {
	Id        string
	Tasks     []Task
	OwnerAddr string
}

// Task Reservation for late-binding
type TaskReservation struct {
	JobID         string
	SchedulerAddr string
	SendTS        time.Time
	RecvTS        time.Time
}

// Struct for collecting time statistics
type TimeStats struct {
	ReserveTime []float64
	QueueTime   []float64
	GetTaskTime []float64
	ServiceTime []float64
}

func (tr *TaskReservation) IsNotEmpty() bool {
	return tr.JobID != "" && tr.SchedulerAddr != ""
}
