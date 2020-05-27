package frontend

import (
	"fmt"
	"math/rand"
	"phoenix"
	"phoenix/types"

	"sync"
)

const ProcessJobsThreads = 4

type Frontend struct {
	Addr 	string

	SchedulerClients []phoenix.TaskSchedulerInterface

	lock sync.Mutex

	// TODO: popoulate this in Frontend
	jobIdToJob map[string]*types.Job

	// False if job is not complete, True if job is complete
	// TODO clean up map
	jobStatus map[string]bool

	sendJobChan chan *types.Job

	jobFinishedChan chan string
	// TODO add other fields
}

func NewFrontend(addr string, schedulerClients []phoenix.TaskSchedulerInterface, jobDoneChan chan string) (phoenix.FrontendInterface, chan *types.Job) {
	fe := &Frontend{
		Addr: addr,
		SchedulerClients: schedulerClients,
		jobIdToJob: make(map[string]*types.Job),
		jobStatus: make(map[string]bool),
		sendJobChan: make(chan *types.Job, ProcessJobsThreads),
		jobFinishedChan: jobDoneChan,
	}

	for i := 0; i < ProcessJobsThreads; i++ {
		go fe.processJobs()
	}

	return fe, fe.sendJobChan
}

func (fe *Frontend) processJobs() {
	for {
		newJob := <- fe.sendJobChan
		fe.lock.Lock()

		scheduler := fe.selectRandScheduler()

		fe.jobStatus[newJob.Id] = false

		// fmt.Printf("[Frontend: processJobs]: Submitting job %s\n", newJob.Id)

		var succ bool
		if e := scheduler.SubmitJob(*newJob, &succ); e != nil || !succ {
			fmt.Errorf("[Frontend: processJobs]: Failed to submit job id:  %s", newJob.Id)
		}
		fe.lock.Unlock()
	}
}

func (fe *Frontend) selectRandScheduler() phoenix.TaskSchedulerInterface {
	idx := rand.Intn(len(fe.SchedulerClients))
	return fe.SchedulerClients[idx]
}

func (fe *Frontend) JobComplete(jobId string, ret *bool) error {

	// fmt.Printf("[Frontend: JobComplete]: Job %s finished\n", jobId)

	fe.lock.Lock()
	defer fe.lock.Unlock()

	fe.jobStatus[jobId] = true

	if fe.jobFinishedChan != nil {
		fe.jobFinishedChan <- jobId
	}

	*ret = true
	return nil
}

var _ phoenix.FrontendInterface = &Frontend{}
