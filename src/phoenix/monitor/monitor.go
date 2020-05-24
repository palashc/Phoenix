package monitor

import (
	"fmt"
	"phoenix"
	"phoenix/executor"
	"phoenix/types"
	"sync"
)

type NodeMonitor struct {
	activeTasks      int
	queue            types.Queue
	lock             sync.Mutex
	executorAddr     string
	schedulerAddrs   []string
	executorClient   phoenix.ExecutorInterface
	schedulerClients map[string]phoenix.TaskSchedulerInterface
	cancelled        map[string]bool
	taskSchedulerMap map[string]string
	jobSchedulerMap  map[string]string
}

const NUM_SLOTS = 4

func NewNodeMonitor(executorClient phoenix.ExecutorInterface, schedulers map[string]phoenix.TaskSchedulerInterface) *NodeMonitor {

	return &NodeMonitor{
		cancelled:        make(map[string]bool),
		taskSchedulerMap: make(map[string]string),
		jobSchedulerMap:  make(map[string]string),
		schedulerClients: schedulers,
		executorClient:   executorClient,
	}
}

/* -------------------------- APIs exposed by NM ------------------------*/

/*
Handles an incoming task reservation from the scheduler.
Gets and launches the task if there is a slot available.
Otherwise, adds the reservation to the queue.
*/
func (nm *NodeMonitor) EnqueueReservation(taskReservation types.TaskReservation, position *int) error {

	nm.lock.Lock()
	defer nm.lock.Unlock()

	if nm.activeTasks < NUM_SLOTS {
		err := nm.getAndLaunchTask(taskReservation)
		if err != nil {
			return err
		}
		nm.activeTasks++
	} else {
		nm.queue.Enqueue(taskReservation)
		*position = nm.queue.Len()
	}

	nm.taskSchedulerMap[taskReservation.JobID] = taskReservation.SchedulerAddr
	return nil
}

/*
Returns number of queued tasks which have not been proactively cancelled.
Will be used by scheduler for probing.
*/
func (nm *NodeMonitor) Probe(_ignore int, n *int) error {

	nm.lock.Lock()
	defer nm.lock.Unlock()

	//TODO: subtract length of `cancelled` map?
	*n = nm.queue.Len() - len(nm.cancelled)
	return nil
}

/*
On a taskComplete() rpc from the executor, calla taskComplete on the scheduler.
Also, attempt to run the next task from the queue.
*/
func (nm *NodeMonitor) TaskComplete(taskID string, ret *bool) error {

	nm.lock.Lock()
	defer nm.lock.Unlock()

	//get the scheduler for the task
	schedulerAddr, ok := nm.taskSchedulerMap[taskID]

	*ret = false
	if !ok {
		return fmt.Errorf("[TaskComplete] Task %v not found", taskID)
	}

	//notify scheduler about task completion
	schedulerClient, err := nm.getSchedulerClient(schedulerAddr)
	if err != nil {
		return fmt.Errorf("[Task Complete] Unable to get a scheduler client: %q", err)
	}
	var succ bool
	err = schedulerClient.TaskComplete(taskID, &succ)
	if err != nil {
		return fmt.Errorf("[Task Complete] Unable to notify scheduler about task completion: %q", err)
	}
	if !succ {
		return fmt.Errorf("[Task Complete] Unable to notify scheduler about task completion")
	}

	nm.activeTasks--

	// launch next task from the queue
	go nm.attemptLaunchTask()

	*ret = true
	return nil
}

/*
Adds the requestID of the proactively cancelled task to a map.
This map is consulted before any task is launched.
*/
func (nm *NodeMonitor) CancelTaskReservation(jobID string, ret *bool) error {

	nm.lock.Lock()
	defer nm.lock.Unlock()

	nm.cancelled[jobID] = true
	*ret = true
	return nil
}

/* -------------------------- Internal APIs of the NM ------------------------*/

/*
Gets the task information from the scheduler for a reservation.
*/
func (nm *NodeMonitor) getTask(taskReservation types.TaskReservation) (types.Task, error) {

	schedulerAddr := taskReservation.SchedulerAddr
	schedulerClient, err := nm.getSchedulerClient(schedulerAddr)
	if err != nil {
		return types.Task{}, fmt.Errorf("[getTask] Unable to get a scheduler client: %q", err)
	}
	jobID := taskReservation.JobID

	var task types.Task
	err = schedulerClient.GetTask(jobID, &task)
	if err != nil {
		return types.Task{}, fmt.Errorf("[getTask] Unable to get task %v from scheduler %v : %q", jobID, schedulerAddr, err)
	}

	return task, nil
}

/*
Launch a task on the application executor.
*/
func (nm *NodeMonitor) launchTask(task types.Task) error {

	var ret bool
	err := nm.executorClient.LaunchTask(task, &ret)
	if err != nil {
		return err
	}
	if !ret {
		return fmt.Errorf("[LaunchTask] Unable to launch task, executor returned false")
	}

	return nil
}

/*
Blocks till a reservation is present in the queue, and then launches it.
*/
func (nm *NodeMonitor) attemptLaunchTask() {

	var taskR types.TaskReservation
	for {
		nm.lock.Lock()
		_taskR := nm.queue.Dequeue()
		nm.lock.Unlock()

		//check if taskR has a reservation for a task which was not cancelled
		if taskR, ok := _taskR.(types.TaskReservation); ok {
			_, cancelled := nm.cancelled[taskR.JobID]
			if !cancelled {
				break
			}
		}
	}

	nm.lock.Lock()
	defer nm.lock.Unlock()

	err := nm.getAndLaunchTask(taskR)
	if err != nil {
		panic("Unable to launch next task")
	}
	nm.activeTasks++

}

/*
Helper method to get a task and launch it
*/
func (nm *NodeMonitor) getAndLaunchTask(taskReservation types.TaskReservation) error {

	task, err := nm.getTask(taskReservation)
	if err != nil {
		return fmt.Errorf("[NM] Unable to get task %v from scheduler: %q", taskReservation.JobID, err)
	}

	nm.taskSchedulerMap[task.Id] = taskReservation.SchedulerAddr

	if task.T > 0 {
		err = nm.launchTask(task)
		if err != nil {
			return fmt.Errorf("[NM] Unable to launch task %v on executor: %q", taskReservation.JobID, err)
		}
	}

	return nil
}

/*
Returns the client for the executor rpc. Creates one if it is nil.
*/
func (nm *NodeMonitor) refreshExecutorClient() error {

	nm.lock.Lock()
	defer nm.lock.Unlock()

	if nm.executorClient == nil {
		executorClient := executor.GetNewClient(nm.executorAddr)
		if executorClient != nil {
			return fmt.Errorf("[RefreshExecutor] Could not instantiate executor client")
		}
		nm.executorClient = executorClient
	}

	return nil
}

/*
Returns the client for the scheduler rpc. Creates one if it is nil.
*/
func (nm *NodeMonitor) getSchedulerClient(addr string) (phoenix.TaskSchedulerInterface, error) {
	return nm.schedulerClients[addr], nil
}

var _ phoenix.MonitorInterface = new(NodeMonitor)
