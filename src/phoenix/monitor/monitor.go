package monitor

import (
	"sync"
	"utils"
)

type NodeMonitor struct {
	activeTasks      int
	queue            Queue
	lock             sync.Mutex
	executor         executorClient
	schedulerClients map[string]SchedulerClient
	cancelled        map[string]bool
}

const NUM_SLOTS = 4

/*
Handles an incoming task reservation from the scheduler.
Gets and launches the task if there is a slot available.
Otherwise, adds the reservation to the queue.
*/
func (nm *NodeMonitor) handleEnqueueReservation(taskReservation *utils.TaskReservation) error {
	panic("todo")
}

/*
Gets the task information from the scheduler for a reservation at the head of the queue.
*/
func (nm *NodeMonitor) getTask(taskReservation *utils.TaskReservation) (utils.Task, error) {
	panic("todo")
}

/*
Launch a task on the application executor.
*/
func (nm *NodeMonitor) launchTask(*utils.Task) error {
	panic("todo")
}

/*
Returns number of queued tasks which have not been proactively cancelled.
Will be used by scheduler for probing.
*/
func (nm *NodeMonitor) getNumQueuedTasks() int {
	return nm.queue.Len() - len(nm.cancelled)
}

/*
On a taskComplete() rpc from the executor, launches a task from the head of the queue.
Otherwise, blocks till a task is enqueued.
*/
func (nm *NodeMonitor) handleTaskComplete() {
	panic("todo")
}

/*
Adds the requestID of the proactively cancelled task to a map.
This map is consulted before any task is launced.
*/
func (nm *NodeMonitor) cancelTaskReservation(requestID string) {
	panic("todo")
}
