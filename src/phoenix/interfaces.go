package phoenix

import "phoenix/types"

type MonitorInterface interface {
	EnqueueReservation(taskReservation *types.TaskReservation, position *int) error
	Probe(_ignore int, n *int) error
	TaskComplete(taskID string, ret *bool) error
	CancelTaskReservation(taskID string, ret *bool) error
}

type TaskSchedulerInterface interface {
	SubmitJob(job types.Job, submitResult *bool) error
	GetTask(taskId string, task *types.Task) error
	NotifyTaskComplete(jobId string, completeResult *bool) error
}

// TODO: Might not necessary to have frontend
type FrontEndInterface interface {
	NotifyCompletion(job types.Job) error
}
