package scheduler

import (
	"phoenix"
	"phoenix/monitor"
	"phoenix/types"
)

const DefaultSampleRatio = 2

type TaskScheduler struct {

	// Monitors that we are able to contact
	MonitorClientPool map[int]monitor.NodeMonitorClient
	AtomicTaskIdCounter uint64

	// From task id -> allocated node monitor
	taskAllocation map[uint64]int

	useCancellation bool

	// Pending, might have a 1-1 mapping from client to scheduler
	// Front-ends that are able to communicate with this scheduler
	//FrontEndClientPool map[int]
}

var _ phoenix.TaskSchedulerInterface = new(TaskScheduler)


func (ts *TaskScheduler) SubmitJob (job types.Job, submitResult *bool) error {




	return nil
}

func (ts *TaskScheduler) GetTask(task types.Task, getTaskResult *bool) error {
	panic("TODO")
}
func (ts *TaskScheduler) NotifyTaskComplete(taskId string, completeResult *bool) error {
	panic("TODO")
}