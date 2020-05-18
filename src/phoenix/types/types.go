package types

import (
	"math/rand"
)

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

// CreateRandTask creates Random-Lengthed Task that lasts between 0 and 1 seconds
// TODO random string for id
func CreateRandTask() Task {
	return Task{T: rand.Float32()}
}

// Task Reservation for late-binding
type TaskReservation struct {
	TaskID      string
	SchedulerID int
}
