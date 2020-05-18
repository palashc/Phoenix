package monitor

import (
	"fmt"
	"phoenix/types"
	"sync"
)

type NodeMonitor struct {
	activeTasks      int
	queue            Queue
	alock            sync.Mutex  //for activeTasks
	qlock            sync.Mutex  //for queue
	mlock            sync.Mutex  //for maps
	executorClient   ExecutorClient
	schedulerClients []SchedulerClient
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

	nm.alock.Lock()
	defer nm.alock.Unlock()

	if activeTasks < NUM_SLOTS {
		task, err := nm.getTask(taskReservation)
		if err != nil {
			return fmt.Errorf("[NM] Unable to get task %v from scheduler: %q", taskReservation.taskID, err)
		}

		err = nm.launchTask(task)
		if err != nil {
			return fmt.Errorf("[NM] Unable to launch task %v on executor: %q", taskReservation.taskID, err)
		}
		nm.activeTasks++
	}
	else {
		nm.qlock.Lock()
		nm.queue.Enqueue(taskReservation)
		nm.qlock.Unlock()
	}

	nm.mlock.Lock()
	nm.taskSchedulerMap[taskReservation.taskID] = taskReservation.schedulerID
	nm.mlock.Unlock()
	return nil
}

/*
Returns number of queued tasks which have not been proactively cancelled.
Will be used by scheduler for probing.
*/
func (nm *NodeMonitor) GetNumQueuedTasks(_ignore int, n *int) error {

	nm.qlock.Lock()
	defer nm.qlock.Unlock()

	//TODO: subtract length of `cancelled` map?
	*n = nm.queue.Len()
	return nil
}

/*
On a taskComplete() rpc from the executor, launches a task from the head of the queue.
Otherwise, blocks till a task is enqueued. Also calls taskComplete on the scheduler.
*/
func (nm *NodeMonitor) HandleTaskComplete(taskID string, ret *bool) error {

	//get the scheduler for the task
	nm.mlock.Lock()
	schedulerID, ok := nm.taskSchedulerMap[taskID]
	nm.mlock.Unlock()

	*ret = false
	if !ok {
		return fmt.Errorf("Task %v not found", taskID)
	}

	//notify scheduler about task completion
	schedulerClient := nm.schedulerClients[schedulerID]
	var succ bool
	err := schedulerClient.TaskComplete(taskID, &succ)
	if err != nil {
		return fmt.Errorf("Unable to notify scheduler about task completion: %q", err)
	}
	if !succ {
		return fmt.Errorf("Unable to notify scheduler about task completion")
	}

	nm.alock.Lock()
	nm.activeTasks--
	nm.alock.Unlock()

	//TODO: launch next task from the queue
	//use go routine so that this rpc does not block?

	*ret = true
	return nil
}

/*
Adds the requestID of the proactively cancelled task to a map.
This map is consulted before any task is launced.
*/
func (nm *NodeMonitor) HandleCancelTaskReservation(taskID string, ret *bool) error {

	nm.mlock.Lock()
	defer nm.mlock.Unlock()

	nm.cancelled[taskID] = true
	*ret = true
	return nil
}

/* -------------------------- Internal APIs of the NM ------------------------*/

/*
Gets the task information from the scheduler for a reservation at the head of the queue.
*/
func (nm *NodeMonitor) getTask(taskReservation *types.TaskReservation) (*types.Task, error) {

	schedulerID := taskReservation.schedulerID
	schedulerClient := nm.schedulerClients[schedulerID]
	taskID := taskReservation.taskID

	var task types.Task
	err := schedulerClient.getTask(taskID, &task)
	if err != nil {
		return nil, fmt.Errorf("Unable to get task %v from scheduler %v : %q", taskID, schedulerID, err)
	}

	return task, nil
}

/*
Launch a task on the application executor.
*/
func (nm *NodeMonitor) launchTask(task *types.Task) error {

	var ret bool
	err := nm.executorClient.LaunchTask(*task, &ret)
	if err != nil {
		return err
	}
	if !ret {
		return fmt.Errorf("Unable to launch task, executor returned false")
	}

	return nil
}
