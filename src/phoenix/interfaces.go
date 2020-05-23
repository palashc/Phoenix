package phoenix

import (
	"phoenix/types"
)

// ExecutorServer interface that all executors must implement
type ExecutorServer interface {

	// Run a specified task
	// net/rpc package mandates 2 parameter RPC calls
	LaunchTask(task types.Task, ret *bool) error
}


type MonitorInterface interface {
	EnqueueReservation(taskReservation *types.TaskReservation, position *int) error
	Probe(_ignore int, n *int) error
	TaskComplete(taskID string, ret *bool) error
	CancelTaskReservation(taskID string, ret *bool) error
}

type SchedulerServer interface {
	TaskComplete(taskID string, ret *bool) error
	GetTask(taskID string, task *types.Task) error
}