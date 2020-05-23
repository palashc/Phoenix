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
	CancelTaskReservation(jobID string, ret *bool) error
}

type TaskSchedulerInterface interface {
	SubmitJob(job types.Job, submitResult *bool) error
	GetTask(taskId string, task *types.Task) error
	TaskComplete(jobId string, completeResult *bool) error
}

// TODO: Might not necessary to have frontend
type FrontEndInterface interface {
	NotifyCompletion(job types.Job) error
}
