package scheduler

import (
	"fmt"
	"math/rand"
	"phoenix"
	"phoenix/types"
	"sync"
	"time"
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

	// pre-computed worker Id array. We can shuffle this for every enqueueJob call
	workerIds []int

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

	workerIds := make([]int, 0)
	for i := 0; i < len(monitorClientPool); i++ {
		workerIds = append(workerIds, i)
	}

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

		// pre-computed list of all workerIds
		workerIds: workerIds,
	}
}

var _ phoenix.TaskSchedulerInterface = new(TaskScheduler)

func (ts *TaskScheduler) SubmitJob(job types.Job, submitResult *bool) error {

	fmt.Printf("[Scheduler: SubmitJob]: Scheduling Job %s with %d\n", job.Id, len(job.Tasks))

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
		// fmt.Println()
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
	//fmt.Println("task complete ", taskId)
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
		fmt.Printf("[Scheduler: TaskComplete]: Job %s finished\n", jobId)
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
		fmt.Println("--------------------Finished job", jobId)
		fmt.Println("jobStatus", ts.jobStatus)
		fmt.Println("taskToJob", ts.taskToJob)
		fmt.Println("jobLeftTask", ts.jobLeftTask)
		fmt.Println("jobMap", ts.jobMap)
		fmt.Println("-------------------", jobId)

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
		}
		queuePos := 0

		targetWorkerId := probeNodesList[targetIndex%len(probeNodesList)]
		if e := ts.MonitorClientPool[targetWorkerId].EnqueueReservation(taskR, &queuePos); e != nil {
			fmt.Printf("[TaskScheduler: enqueueJob]: Failed to enqueue reservation on %d\n", targetWorkerId)
		}

		fmt.Printf("[TaskScheduler: enqueueJob]: Enqueuing reservation on monitor %d for job reservation %s\n",
			targetWorkerId, taskR.JobID)

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

func (ts *TaskScheduler) selectEnqueueWorker(probeCount int) []int {

	probeNodesList := make([]int, 0)

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(ts.workerIds), func(i, j int) {
		ts.workerIds[i], ts.workerIds[j] = ts.workerIds[j], ts.workerIds[i]
	})

	for i := 0; i < probeCount; i++ {
		targetWorkerId := ts.workerIds[i%len(ts.MonitorClientPool)]
		probeNodesList = append(probeNodesList, targetWorkerId)
	}

	return probeNodesList
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
