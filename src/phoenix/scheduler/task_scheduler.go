package scheduler

import (
	"fmt"
	"math/rand"
	"phoenix"
	"phoenix/monitor"
	"phoenix/types"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const DefaultSampleRatio = 2

type TaskScheduler struct {
	Addr string

	// map of addresses to Monitors that we are able to contact
	MonitorClientPool map[string]phoenix.MonitorInterface

	// addresses of our workers
	workerIds []string

	// map of worker address to current tasks assigned to worker
	workerIdToTask map[string]map[string]bool

	// lock around MonitorClientPool, workerIds, and workerIdToTask
	workerLock sync.Mutex

	// Frontend Client Pool
	FrontendClientPool map[string]phoenix.FrontendInterface

	// maps jobIds to a map of taskIds to TaskRecords
	jobStatus     map[string]map[string]*types.TaskRecord
	jobStatusLock sync.Mutex

	// tasks left in a job
	jobLeftTask map[string]int

	// jobId to Job
	jobMap     map[string]types.Job
	jobMapLock sync.Mutex

	// taskId to JobId
	taskToJob     map[string]string
	taskToJobLock sync.Mutex
	// Pending, might have a 1-1 mapping from client to scheduler
	// Front-ends that are able to communicate with this scheduler
	//FrontEndClientPool map[int]

	// zookeeper connection
	zkConn *zk.Conn
}

var _ phoenix.TaskSchedulerInterface = new(TaskScheduler)

func NewTaskScheduler(addr string, zkHostPorts []string, frontendClientPool map[string]phoenix.FrontendInterface) phoenix.TaskSchedulerInterface {

	ts := &TaskScheduler{
		FrontendClientPool: frontendClientPool,
		jobStatusLock:      sync.Mutex{},
		jobStatus:          make(map[string]map[string]*types.TaskRecord),
		jobLeftTask:        make(map[string]int),
		jobMapLock:         sync.Mutex{},
		jobMap:             make(map[string]types.Job),
		taskToJobLock:      sync.Mutex{},
		taskToJob:          make(map[string]string),
		Addr:               addr,

		// map addresses to clients
		MonitorClientPool: make(map[string]phoenix.MonitorInterface, 0),
		workerIds:         []string{},
		workerIdToTask:    make(map[string]map[string]bool),
		workerLock:        sync.Mutex{},
	}

	ready := make(chan bool)
	// dynamically update monitorClientPool
	// dynamically update workerIds
	go ts.watchWorkerNodes(zkHostPorts, ready)

	// wait for scheduler to find at least one living worker
	<-ready

	return ts
}

func (ts *TaskScheduler) watchWorkerNodes(zkHostPorts []string, ready chan bool) {

	var err error
	ts.zkConn, _, err = zk.Connect(zkHostPorts, phoenix.ZK_MONITOR_CONNECTION_TIMEOUT)
	if err != nil {
		fmt.Println("[NodeMonitor: registerMonitorZK] Unable to connect to Zookeeper!")
		panic(err)
	}

	workerNodeExists, _, err := ts.zkConn.Exists(phoenix.ZK_WORKER_NODE_PATH)
	if !workerNodeExists || err != nil {
		_, e := ts.zkConn.Create(phoenix.ZK_WORKER_NODE_PATH, []byte{0}, 0, zk.WorldACL(zk.PermAll))
		if e != nil {
			fmt.Printf("Error: %v\n Could not create Worker Node Path node at %s\n", e, phoenix.ZK_WORKER_NODE_PATH)
		}
	}

	for {
		children, _, eventChannel, err := ts.zkConn.ChildrenW(phoenix.ZK_WORKER_NODE_PATH)
		fmt.Println("[TaskScheduler: watchWorkerNodes] children: ", children)
		ts.rescheduleLostTasks(children)

		if err != nil {
			panic(fmt.Errorf("[TaskScheduler: watchWorkerNodes] error in getting children of %s: %v",
				phoenix.ZK_WORKER_NODE_PATH, err))
		}

		if len(children) > 0 && ready != nil {
			ready <- true
			ready = nil
		}

		// wait for event
		<-eventChannel
	}

}

func (ts *TaskScheduler) rescheduleLostTasks(children []string) {

	// create new client pool and newWorkerIds
	newClientPool := make(map[string]phoenix.MonitorInterface)
	newWorkerIds := make([]string, len(children))
	for idx, childAddress := range children {
		newClientPool[childAddress] = monitor.GetNewClient(childAddress)
		newWorkerIds[idx] = childAddress
	}

	for oldWorkerAddr := range ts.MonitorClientPool {
		// reschedule lost tasks
		_, exists := newClientPool[oldWorkerAddr]
		if !exists {
			ts.workerLock.Lock()
			taskMap := ts.workerIdToTask[oldWorkerAddr]
			ts.workerLock.Unlock()

			for taskId := range taskMap {

				// extract corresponding jobId
				ts.taskToJobLock.Lock()
				jobId := ts.taskToJob[taskId]
				ts.taskToJobLock.Unlock()

				// set assignedWorker to false
				ts.jobStatusLock.Lock()
				ts.jobStatus[jobId][taskId].AssignedWorker = 0
				ts.jobStatusLock.Unlock()
			}

			// clean up map
			ts.workerLock.Lock()
			delete(ts.workerIdToTask, oldWorkerAddr)
			ts.workerLock.Unlock()
		}
	}

	// update client pool and workerIds
	ts.workerLock.Lock()
	ts.MonitorClientPool = newClientPool
	ts.workerIds = newWorkerIds
	ts.workerLock.Unlock()

	// fmt.Println("[TaskScheduler: rescheduleLostTasks]: workerLock released")
}

func (ts *TaskScheduler) SubmitJob(job types.Job, submitResult *bool) error {

	// fmt.Printf("[TaskScheduler %s: SubmitJob]: Scheduling Job %s with %d\n", ts.Addr, job.Id, len(job.Tasks))

	enqueueCount := len(job.Tasks) * DefaultSampleRatio
	ts.jobMapLock.Lock()
	ts.jobMap[job.Id] = job
	ts.jobMapLock.Unlock()

	// Add currentJob to status map
	ts.jobStatusLock.Lock()
	currJobStatus, found := ts.jobStatus[job.Id]
	if found {
		*submitResult = false
		return fmt.Errorf("[SubmitJob] Repeated JobId %v\n", job.Id)
	} else {
		currJobStatus = make(map[string]*types.TaskRecord)
		ts.jobStatus[job.Id] = currJobStatus
	}
	ts.jobLeftTask[job.Id] = len(job.Tasks)
	ts.jobStatusLock.Unlock()

	// Add records for the tasks in current job for future scheduling
	ts.taskToJobLock.Lock()
	for _, task := range job.Tasks {
		currJobStatus[task.Id] = &types.TaskRecord{Task: task, AssignedWorker: -1, Finished: false}
		ts.taskToJob[task.Id] = job.Id
	}
	ts.taskToJobLock.Unlock()

	e := ts.enqueueJob(enqueueCount, job.Id)
	if e != nil {
		*submitResult = false
		return fmt.Errorf("[SubmitJob] Failed to submit jobs for %v\n", job.Id)
	}

	*submitResult = true
	return nil
}

func (ts *TaskScheduler) GetTask(jobId string, taskRequest *types.TaskRequest) error {

	ts.jobMapLock.Lock()
	targetJob := ts.jobMap[jobId]
	ts.jobMapLock.Unlock()

	for _, pendingTask := range targetJob.Tasks {
		ts.jobStatusLock.Lock()
		taskRecord := ts.jobStatus[jobId][pendingTask.Id]
		ts.jobStatusLock.Unlock()

		// Skip the task that has been assigned to some worker or already done
		if taskRecord.AssignedWorker != -1 || taskRecord.Finished {
			continue
		}

		taskRequest.Task = &pendingTask

		ts.workerLock.Lock()
		// fmt.Println("[TaskScheduler: GetTask]: worker lock acquired")

		// pendingTask is now inflight at workerIdToTask
		_, exists := ts.workerIdToTask[taskRequest.WorkerAddr]
		if !exists {
			ts.workerIdToTask[taskRequest.WorkerAddr] = make(map[string]bool)
		}

		ts.workerIdToTask[taskRequest.WorkerAddr][pendingTask.Id] = true

		// fmt.Println("[TaskScheduler: GetTask]: worker lock released")
		ts.workerLock.Unlock()

		//TODO: Need to update in the future or change it to Assigned boolean value
		taskRecord.AssignedWorker = 0
		// fmt.Println()
		break
		// TODO: Record which backend got assigned for this task
		//ts.taskAllocationLock.Lock()
		//ts.taskAllocation[jobId][pendingTask.Id] =
		//ts.taskAllocationLock.Unlock()
	}

	// No task got assigned
	if taskRequest.Task == nil {
		// TODO: I need to know who's the requester to call cancellation.
		// requester it taskRequest.AssignedWorker
	}

	return nil
}

func (ts *TaskScheduler) TaskComplete(msg types.WorkerTaskCompleteMsg, completeResult *bool) error {

	fmt.Println("[TaskScheduler: TaskComplete] Task has completed: ", msg)

	taskId, workerAddr := msg.TaskID, msg.WorkerAddr

	ts.taskToJobLock.Lock()
	jobId, found := ts.taskToJob[taskId]
	if !found {
		*completeResult = true
		ts.taskToJobLock.Unlock()
		return fmt.Errorf("[TaskScheduler: TaskComplete] Notify complete received, but job can't found for task %v\n", taskId)
	}
	ts.taskToJobLock.Unlock()

	ts.workerLock.Lock()
	// taskId is no longer in flight at workerAddr
	delete(ts.workerIdToTask[workerAddr], taskId)
	ts.workerLock.Unlock()

	ts.jobStatusLock.Lock()
	currTaskRecord := ts.jobStatus[jobId][taskId]
	currTaskRecord.Finished = true
	leftJob := ts.jobLeftTask[jobId] - 1
	ts.jobLeftTask[jobId] = leftJob
	ts.jobStatusLock.Unlock()

	// Clean things up when the job is finished
	if leftJob == 0 {
		fmt.Printf("[TaskScheduler %s: TaskComplete]: Job %s finished\n", ts.Addr, jobId)
		// Only nested lock here. Other place don't have nested lock to avoid deadlock
		ts.jobStatusLock.Lock()
		ts.jobMapLock.Lock()

		for _, task := range ts.jobMap[jobId].Tasks {
			if _, found := ts.jobStatus[jobId][task.Id]; found {
				ts.taskToJobLock.Lock()
				delete(ts.taskToJob, task.Id)
				ts.taskToJobLock.Unlock()
			}
		}

		// Tell Frontend job has finished
		go func(jId, feAddr string) {
			var succ bool
			if e := ts.FrontendClientPool[feAddr].JobComplete(jId, &succ); e != nil || !succ {
				fmt.Printf("What's the error: %v\n", e)
				fmt.Printf("[TaskScheduler: TaskComplete]: Error in telling frontend at %s that job %s has finished\n",
					feAddr, jId)
			}
		}(jobId, ts.jobMap[jobId].OwnerAddr)

		delete(ts.jobLeftTask, jobId)
		delete(ts.jobStatus, jobId)
		delete(ts.jobMap, jobId)
		ts.jobMapLock.Unlock()
		ts.jobStatusLock.Unlock()

		// TODO: Notify the corresponding frontend that the task is finished
		// fmt.Println("--------------------Finished job", jobId)
		// fmt.Println("jobStatus", ts.jobStatus)
		// fmt.Println("taskToJob", ts.taskToJob)
		// fmt.Println("jobLeftTask", ts.jobLeftTask)
		// fmt.Println("jobMap", ts.jobMap)
		// fmt.Println("-------------------", jobId)
	}

	*completeResult = true
	return nil
}

func (ts *TaskScheduler) enqueueJob(enqueueCount int, jobId string) error {
	//nodesToEnqueue := ts.selectEnqueueWorker(enqueueCount)

	probeNodesList := ts.selectEnqueueWorker(enqueueCount)
	targetIndex := 0

	for enqueueCount > 0 {
		taskR := types.TaskReservation{
			JobID:         jobId,
			SchedulerAddr: ts.Addr,
			SendTS:        time.Now(),
		}
		queuePos := 0

		targetWorkerId := probeNodesList[targetIndex%len(probeNodesList)]

		ts.workerLock.Lock()
		// fmt.Println("[TaskScheduler: enqueueJob] workerLock acquired")
		targetMonitor := ts.MonitorClientPool[targetWorkerId]
		// fmt.Println("[TaskScheduler: enqueueJob] workerLock Unlocked")
		ts.workerLock.Unlock()

		if e := targetMonitor.EnqueueReservation(taskR, &queuePos); e != nil {
			fmt.Printf("[TaskScheduler %s: enqueueJob]: Failed to enqueue reservation on %s: %v\n",
				ts.Addr, targetWorkerId, e)
		}

		fmt.Printf("[TaskScheduler %s: enqueueJob]: Enqueuing reservation on monitor %d for job reservation %s\n",
			ts.Addr, targetWorkerId, taskR.JobID)

		// if e != nil {
		// 	// Remove the inactive back
		// 	probeNodesList = append(probeNodesList[:targetIndex], probeNodesList[targetIndex+1:]...)
		// 	continue
		// }

		enqueueCount--
		targetIndex++
	}

	return nil
}

func (ts *TaskScheduler) selectEnqueueWorker(probeCount int) []string {

	// TODO: maybe we can be more conservative with locks
	// locks around modifying workerIds
	ts.workerLock.Lock()
	// fmt.Println("[TaskScheduler: selectEnqueueWorker]: workerLock acquired")

	defer ts.workerLock.Unlock()

	probeNodesList := make([]string, 0)

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(ts.workerIds), func(i, j int) {
		ts.workerIds[i], ts.workerIds[j] = ts.workerIds[j], ts.workerIds[i]
	})

	for i := 0; i < probeCount; i++ {
		targetWorkerId := ts.workerIds[i%len(ts.MonitorClientPool)]
		probeNodesList = append(probeNodesList, targetWorkerId)
	}

	// fmt.Println("[TaskScheduler: selectEnqueueWorker]: workerLock released")
	return probeNodesList
}
