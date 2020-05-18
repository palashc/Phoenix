package monitor

import (
	"fmt"
	"phoenix/executor"
	"phoenix/scheduler"
	"phoenix/types"
	"sync"
)

type NodeMonitor struct {
	activeTasks      int
	queue            Queue
	alock            sync.Mutex //for activeTasks
	qlock            sync.Mutex //for queue
	mlock            sync.Mutex //for maps
	executorClient   executor.ExecutorClient
	schedulerClients []scheduler.SchedulerClient
	cancelled        map[string]bool
	taskSchedulerMap map[string]int
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

	if nm.activeTasks < NUM_SLOTS {
		err := nm.getAndLaunchTask(taskReservation)
		if err != nil {
			return err
		}
		nm.activeTasks++
	} else {
		nm.qlock.Lock()
		nm.queue.Enqueue(taskReservation)
		*position = nm.queue.Len()
		nm.qlock.Unlock()
	}

	nm.mlock.Lock()
	nm.taskSchedulerMap[taskReservation.TaskID] = taskReservation.SchedulerID
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
On a taskComplete() rpc from the executor, calla taskComplete on the scheduler.
Also, attempt to run the next task from the queue.
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

	// launch next task from the queue
	go nm.attemptLaunchTask()

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

	schedulerID := taskReservation.SchedulerID
	schedulerClient := nm.schedulerClients[schedulerID]
	taskID := taskReservation.TaskID

	var task types.Task
	err := schedulerClient.getTask(taskID, &task)
	if err != nil {
		return nil, fmt.Errorf("Unable to get task %v from scheduler %v : %q", taskID, schedulerID, err)
	}

	return &task, nil
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

/*
Blocks till a reservation is present in the queue, and then launches it.
*/
func (nm *NodeMonitor) attemptLaunchTask() {

	var taskR types.TaskReservation
	for {
		nm.qlock.Lock()
		_taskR := nm.queue.Dequeue()
		nm.qlock.Unlock()

		if taskR, ok := _taskR.(types.TaskReservation); ok {
			break
		}
	}

	nm.alock.Lock()
	defer nm.alock.Unlock()

	err := nm.getAndLaunchTask(&taskR)
	if err != nil {
		panic("Unable to launch next task")
	}
	nm.activeTasks++

}

/*
Helper method to get a task and launch it
*/
func (nm *NodeMonitor) getAndLaunchTask(taskReservation *types.TaskReservation) error {

	task, err := nm.getTask(taskReservation)
	if err != nil {
		return fmt.Errorf("[NM] Unable to get task %v from scheduler: %q", taskReservation.TaskID, err)
	}

	err = nm.launchTask(task)
	if err != nil {
		return fmt.Errorf("[NM] Unable to launch task %v on executor: %q", taskReservation.TaskID, err)
	}

	return nil
}
