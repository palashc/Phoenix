package phoenix

import (
	"phoenix/types"
)

type FrontendInterface interface {
	JobComplete(jobId string, ret *bool) error
}

// ExecutorServer interface that all executors must implement
type ExecutorInterface interface {

	// Run a specified task
	// net/rpc package mandates 2 parameter RPC calls
	LaunchTask(task types.Task, ret *bool) error
}

type MonitorInterface interface {
	EnqueueReservation(taskReservation types.TaskReservation, position *int) error
	Probe(_ignore int, n *int) error
	TaskComplete(taskID string, ret *bool) error
	CancelTaskReservation(jobID string, ret *bool) error
}

type TaskSchedulerInterface interface {
	SubmitJob(job types.Job, submitResult *bool) error
	GetTask(jobId string, taskRequest *types.TaskRequest) error
	TaskComplete(msg types.WorkerTaskCompleteMsg, completeResult *bool) error
}
