package types

// Task simulates Phoenix tasks with a sleep time between 0 and 1 seconds

type TaskID string

type Task struct {
	JobId string
	Id    string
	T     float64
}

type TaskRequest struct {
	Task       *Task
	WorkerAddr string
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
}

func (tr *TaskReservation) IsNotEmpty() bool {
	return tr.JobID != "" && tr.SchedulerAddr != ""
}
