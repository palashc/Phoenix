package monitor

import (
	"fmt"
	"path"

	"phoenix"
	"phoenix/types"

	"github.com/golang-collections/collections/queue"
	"github.com/samuel/go-zookeeper/zk"

	"sync"
)

type NodeMonitor struct {
	addr             string
	activeTasks      int
	queue            queue.Queue
	lock             sync.Mutex
	executorAddr     string
	schedulerAddrs   []string
	executorClient   phoenix.ExecutorInterface
	schedulerClients map[string]phoenix.TaskSchedulerInterface
	cancelled        map[string]bool
	taskSchedulerMap map[string]string
	jobSchedulerMap  map[string]string
	launchCond       *sync.Cond
	slotCount        int
}

func NewNodeMonitor(slotCount int, executorClient phoenix.ExecutorInterface,
	schedulers map[string]phoenix.TaskSchedulerInterface, zkHostPorts []string, addr string) *NodeMonitor {

	nm := &NodeMonitor{
		addr:             addr,
		cancelled:        make(map[string]bool),
		taskSchedulerMap: make(map[string]string),
		jobSchedulerMap:  make(map[string]string),
		schedulerClients: schedulers,
		executorClient:   executorClient,
		launchCond:       sync.NewCond(&sync.Mutex{}),
		slotCount:        slotCount,
	}

	nm.registerMonitorZK(zkHostPorts)
	go nm.taskLauncher()

	return nm
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

	// fmt.Printf("[Monitor: EnqueueReservation]: adding task reservation for job: %s to queue\n",
	//		taskReservation.JobID)

	nm.queue.Enqueue(taskReservation)
	nm.launchCond.Signal()

	*position = nm.queue.Len()

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

	*n = nm.queue.Len() - len(nm.cancelled)
	return nil
}

/*
On a taskComplete() rpc from the executor, calla taskComplete on the scheduler.
Also, attempt to run the next task from the queue.
*/
func (nm *NodeMonitor) TaskComplete(taskID string, ret *bool) error {
	//fmt.Println("task done ", taskID)
	nm.lock.Lock()
	defer nm.lock.Unlock()

	// fmt.Printf("[Monitor: TaskComplete]: task %s marked as complete\n", taskID)

	//get the scheduler for the task
	schedulerAddr, ok := nm.taskSchedulerMap[taskID]

	*ret = false
	if !ok {
		return fmt.Errorf("[TaskComplete] Task %v not found", taskID)
	}

	//notify scheduler about task completion
	schedulerClient, ok := nm.schedulerClients[schedulerAddr]
	if !ok {
		return fmt.Errorf("[Task Complete] Unable to get a scheduler client")
	}
	var succ bool
	err := schedulerClient.TaskComplete(taskID, &succ)
	if err != nil {
		return fmt.Errorf("[Task Complete] Unable to notify scheduler about task completion: %q", err)
	}
	if !succ {
		return fmt.Errorf("[Task Complete] Unable to notify scheduler about task completion")
	}

	nm.activeTasks--
	nm.launchCond.Signal()

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
	schedulerClient, ok := nm.schedulerClients[schedulerAddr]
	if !ok {
		return types.Task{}, fmt.Errorf("[getTask] Unable to get a scheduler client")
	}
	jobID := taskReservation.JobID

	var task types.Task
	err := schedulerClient.GetTask(jobID, &task)
	if err != nil {
		return types.Task{}, fmt.Errorf("[getTask] Unable to get task %v from scheduler %v : %q", jobID, schedulerAddr, err)
	}

	return task, nil
}

/*
Launch a task on the application executor.
*/
func (nm *NodeMonitor) launchTask(task types.Task, reservation types.TaskReservation) error {
	nm.lock.Lock()
	defer nm.lock.Unlock()

	// fmt.Println("[Monitor: launchTask]: Now calling executor to launch ", task)
	var ret bool
	err := nm.executorClient.LaunchTask(task, &ret)
	if err != nil {
		return err
	}
	if !ret {
		return fmt.Errorf("[LaunchTask] Unable to launch task, executor returned false")
	}

	nm.taskSchedulerMap[task.Id] = reservation.SchedulerAddr

	nm.activeTasks++

	return nil
}

/*
Blocks till a reservation is present in the queue, and then launches it.
*/
func (nm *NodeMonitor) attemptLaunchTask() {

	_taskR := nm.queue.Dequeue()

	//check if taskR has a reservation for a task which was not cancelled
	if taskR, ok := _taskR.(types.TaskReservation); ok {
		_, cancelled := nm.cancelled[taskR.JobID]
		if !cancelled && taskR.IsNotEmpty() {
			if taskR.IsNotEmpty() {
				// fmt.Printf("[Monitor: attemptLaunchTask]: launching %s\n", taskR.JobID)
				// TODO: Parallelize with goroutines
				err := nm.getAndLaunchTask(taskR)
				if err != nil {
					panic("Unable to launch next task")
				}
			}
		}
	}
}

/*
Helper method to get a task and launch it
*/
func (nm *NodeMonitor) getAndLaunchTask(taskReservation types.TaskReservation) error {

	task, err := nm.getTask(taskReservation)
	if err != nil {
		return fmt.Errorf("[NM] Unable to get task %v from scheduler: %q", taskReservation.JobID, err)
	}

	// fmt.Println("[Monitor getAndLaunchTask]: Task fetched", taskReservation, task)

	if task.T > 0 {
		err = nm.launchTask(task, taskReservation)
		if err != nil {
			return fmt.Errorf("[NM] Unable to launch task %v on executor: %q", taskReservation.JobID, err)
		}
	}

	return nil
}

/*
Task Launcher: polls the queue and launches task if a slot is ready
*/
func (nm *NodeMonitor) taskLauncher() {

	for {
		nm.launchCond.L.Lock()
		for nm.activeTasks >= nm.slotCount || nm.queue.Len() == 0 {
			nm.launchCond.Wait()
		}

		// fmt.Println("[Monitor: TaskLauncher] About to attempt launch task, active tasks: ", nm.activeTasks)
		// fmt.Println("[Monitor: TaskLauncher] queueSize: ", nm.queue.Len())
		nm.attemptLaunchTask()

		nm.launchCond.L.Unlock()
	}
}

/*
Register this monitor on Zookeeper by creating an ephemeral ZNode.
*/
func (nm *NodeMonitor) registerMonitorZK(zkHostPorts []string) {

	//connect to ZK
	conn, _, err := zk.Connect(zkHostPorts, phoenix.ZK_MONITOR_CONNECTION_TIMEOUT)
	if err != nil {
		fmt.Println("[NodeMonitor: registerMonitorZK] Unable to connect to Zookeeper!")
		panic(err)
	}

	//Create ephemeral node
	monitorPath := path.Join(phoenix.ZK_WORKER_NODE_PATH, nm.addr)
	data := []byte(nm.addr)
	acls := []zk.ACL{}
	acl := zk.ACL{Perms: zk.PermAll, Scheme: "world", ID: "anyone"}
	acls = append(acls, acl)
	_, err = conn.Create(monitorPath, data, zk.FlagEphemeral, acls)
	if err != nil {
		fmt.Println("[NodeMonitor: registerMonitorZK] Unable to create znode!")
		panic(err)
	}

}

var _ phoenix.MonitorInterface = new(NodeMonitor)
