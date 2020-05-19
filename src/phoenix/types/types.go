package types

// Task simulates Phoenix tasks with a sleep time between 0 and 1 seconds
type Task struct {
	id string
	T  float32
}

// Job is an array of Tasks
type Job struct {
	id    string
	tasks []Task
}

// Task Reservation for late-binding
type TaskReservation struct {
	TaskID        string
	SchedulerAddr string
}
