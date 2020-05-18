package monitor

import (
	"phoenix/types"
	"sync"
)

type NodeMonitor struct {
	activeTasks      int
	queue            Queue
	lock             sync.Mutex
	executorClient   ExecutorClient
	schedulerClients map[string]SchedulerClient
	cancelled        map[string]bool
	taskSchedulerMap map[string]string
}

const NUM_SLOTS = 4

/* -------------------------- APIs exposed by NM ------------------------*/

/*
Handles an incoming task reservation from the scheduler.
Gets and launches the task if there is a slot available.
Otherwise, adds the reservation to the queue.
*/
func (nm *NodeMonitor) HandleEnqueueReservation(taskReservation *types.TaskReservation, position *int) error {
	panic("todo")
}

/*
Returns number of queued tasks which have not been proactively cancelled.
Will be used by scheduler for probing.
*/
func (nm *NodeMonitor) GetNumQueuedTasks(_ignore int, n *int) int {
	*n = nm.queue.Len() - len(nm.cancelled)
}

/*
On a taskComplete() rpc from the executor, launches a task from the head of the queue.
Otherwise, blocks till a task is enqueued.
*/
func (nm *NodeMonitor) HandleTaskComplete(taskID string, ret *bool) error {
	panic("todo")
}

/*
Adds the requestID of the proactively cancelled task to a map.
This map is consulted before any task is launced.
*/
func (nm *NodeMonitor) HandleCancelTaskReservation(taskID string, ret *bool) {
	panic("todo")
}

/* -------------------------- Internal APIs of the NM ------------------------*/

/*
Gets the task information from the scheduler for a reservation at the head of the queue.
*/
func (nm *NodeMonitor) getTask(taskReservation *types.TaskReservation) (*types.Task, error) {
	panic("todo")
}

/*
Launch a task on the application executor.
*/
func (nm *NodeMonitor) launchTask(*types.Task) error {
	panic("todo")
}
