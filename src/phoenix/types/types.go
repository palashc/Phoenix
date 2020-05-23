package types

// Task simulates Phoenix tasks with a sleep time between 0 and 1 seconds
type Task struct {
	JobId string
	Id    string
	T     float32
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
	ownerAddr string
}

// Task Reservation for late-binding
type TaskReservation struct {
	TaskID        string
	SchedulerAddr string
}
