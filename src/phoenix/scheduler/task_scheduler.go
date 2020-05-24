package scheduler

import (
	"fmt"
	"math/rand"
	"phoenix"
	"phoenix/types"
	"sync"
)

const DefaultSampleRatio = 2

func MIN(i, j int) int {
	if i < j {
		return i
	}
	return j
}

type TaskScheduler struct {
	Addr string

	// Monitors that we are able to contact
	MonitorClientPool map[int]phoenix.MonitorInterface

	// From JobId -> task id -> allocated node monitor
	//taskAllocationLock sync.Mutex
	//taskAllocation map[string]map[string]int

	jobStatusLock sync.Mutex
	jobStatus     map[string]map[string]*types.TaskRecord
	jobLeftTask   map[string]int

	jobMapLock sync.Mutex
	jobMap     map[string]types.Job

	taskToJobLock sync.Mutex
	taskToJob     map[string]string
	// Pending, might have a 1-1 mapping from client to scheduler
	// Front-ends that are able to communicate with this scheduler
	//FrontEndClientPool map[int]
}

func NewTaskScheduler(addr string, monitorClientPool map[int]phoenix.MonitorInterface) phoenix.TaskSchedulerInterface {
	return &TaskScheduler{
		MonitorClientPool: monitorClientPool,
		jobStatusLock:     sync.Mutex{},
		jobStatus:         make(map[string]map[string]*types.TaskRecord),
		jobLeftTask:       make(map[string]int),
		jobMapLock:        sync.Mutex{},
		jobMap:            make(map[string]types.Job),
		taskToJobLock:     sync.Mutex{},
		taskToJob:         make(map[string]string),
		Addr:              addr,
	}
}

var _ phoenix.TaskSchedulerInterface = new(TaskScheduler)

func (ts *TaskScheduler) SubmitJob(job types.Job, submitResult *bool) error {

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

func (ts *TaskScheduler) GetTask(jobId string, task *types.Task) error {

	ts.jobMapLock.Lock()
	targetJob := ts.jobMap[jobId]
	ts.jobMapLock.Unlock()

	ts.jobStatusLock.Lock()
	for _, pendingTask := range targetJob.Tasks {
		taskRecord := ts.jobStatus[jobId][pendingTask.Id]

		// Skip the task that has been assigned to some worker or already done
		if taskRecord.AssignedWorker != -1 || taskRecord.Finished {
			continue
		}

		*task = pendingTask
		//TODO: Need to update in the future or change it to Assigned boolean value
		taskRecord.AssignedWorker = 0
		break
		// TODO: Record which backend got assigned for this task
		//ts.taskAllocationLock.Lock()
		//ts.taskAllocation[jobId][pendingTask.Id] =
		//ts.taskAllocationLock.Unlock()
	}
	ts.jobStatusLock.Unlock()

	// No task got assigned
	if task == nil {
		// TODO: I need to know who's the requester to call cancellation.
	}

	return nil
}

func (ts *TaskScheduler) TaskComplete(taskId string, completeResult *bool) error {

	ts.taskToJobLock.Lock()
	jobId, found := ts.taskToJob[taskId]
	if !found {
		*completeResult = true
		ts.taskToJobLock.Unlock()
		return fmt.Errorf("[Sched] Notify complete received, but job can't found for task %v\n", taskId)
	}
	ts.taskToJobLock.Unlock()

	ts.jobStatusLock.Lock()
	currTaskRecord := ts.jobStatus[jobId][taskId]
	currTaskRecord.Finished = true
	leftJob := ts.jobLeftTask[jobId] - 1
	ts.jobLeftTask[jobId] = leftJob
	ts.jobStatusLock.Unlock()

	// Clean things up when the job is finished
	if leftJob == 0 {
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

		delete(ts.jobLeftTask, jobId)
		delete(ts.jobStatus, jobId)
		delete(ts.jobMap, jobId)
		ts.jobMapLock.Unlock()
		ts.jobStatusLock.Unlock()

		// TODO: Notify the corresponding frontend that the task is finished
	}

	*completeResult = true
	return nil
}

func (ts *TaskScheduler) enqueueJob(enqueueCount int, jobId string) error {
	nodesToEnqueue := ts.selectEnqueueWorker(enqueueCount)

	probeNodesList := MapToList(nodesToEnqueue)
	targetIndex := 0

	for enqueueCount > 0 {
		taskR := types.TaskReservation{
			JobID:         jobId,
			SchedulerAddr: ts.Addr,
		}
		queuePos := 0

		targetWorkerId := probeNodesList[targetIndex%len(probeNodesList)]
		_ = ts.MonitorClientPool[targetWorkerId].EnqueueReservation(taskR, &queuePos)

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

func (ts *TaskScheduler) selectEnqueueWorker(probeCount int) map[int]bool {

	probeNodes := make(map[int]bool)

	for probeCount > 0 {

		targetWorkerId := rand.Int() % len(ts.MonitorClientPool)

		// 		if _, found := probeNodes[targetWorkerId]; found {
		// 			continue
		// 		}

		//var _ignore, queueLength int
		//e := ts.MonitorClientPool[targetWorkerId].Probe(_ignore, &queueLength)
		//if e != nil {
		//	continue
		//}

		probeNodes[targetWorkerId] = true
		probeCount--
	}

	return probeNodes
}

func MapToList(nodeMap map[int]bool) (resultList []int) {

	resultList = []int{}

	for nodeId, _ := range nodeMap {
		resultList = append(resultList, nodeId)
	}

	return
}

//SubmitJob(job types.Job, submitResult *bool) error
//GetTask(taskId string, task *types.Task) error
//TaskComplete(taskId string, completeResult *bool) error
