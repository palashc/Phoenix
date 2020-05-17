package utils

import (
	"math/rand"
)

// Task simulates Phoenix tasks with a sleep time between 0 and 1 seconds
type Task float32

// Job is an array of Tasks
type Job []Task

// CreateRandTask creates Random-Lengthed Task that lasts between 0 and 1 seconds
func CreateRandTask() Task {
	return Task(rand.Float32())
}

// Task Reservation for late-binding
type TaskReservation struct {
	requestID   string
	schedulerID string
}
