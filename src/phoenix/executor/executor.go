package executor

import (
	"fmt"

	"phoenix"
	"phoenix/types"

	"sync"
)

// Logging option for Executor struct
var Logging bool

// ECState implements ExecutorServer
type ECState struct {
	id int

	// worker count
	slotCount int

	// running workers
	availWorkers []bool

	// channels for communicating with workers
	taskChans []chan *types.Task

	// buffered channel should have slotCount size
	doneChan chan taskDone

	// lock
	lock sync.Mutex

	// nmClient
	nmClient phoenix.MonitorInterface
}

// NewExecutor launches the executor backend
// Pass an NmClient instead of the address so executor doesn't have to depend on phoenix/monitor and cause
// an import cycle
func NewExecutor(myID int, mySlotCount int, myNmClient phoenix.MonitorInterface) phoenix.ExecutorInterface {

	ec := &ECState{
		id:           myID,
		slotCount:    mySlotCount,
		availWorkers: make([]bool, mySlotCount),
		taskChans:    make([]chan *types.Task, mySlotCount),
		doneChan:     make(chan taskDone, mySlotCount),
		nmClient:     myNmClient,
	}

	// initialize worker pool
	ec.initWorkerPool()

	return ec
}

// Init ...
func (ec *ECState) initWorkerPool() {
	ec.lock.Lock()
	defer ec.lock.Unlock()

	// launch all workers
	for wID := 0; wID < ec.slotCount; wID++ {
		ec.taskChans[wID] = make(chan *types.Task)
		go Worker(ec.id, wID, ec.taskChans[wID], ec.doneChan)

		ec.availWorkers[wID] = true
	}

	// launch long-living worker coordinator
	go ec.workerCoordinator()
}

// WorkerCoordinator updates availWorkers
// Do Not export method
func (ec *ECState) workerCoordinator() {
	for {
		// find worker that has finished; block on doneChannel
		finishedTask := <-ec.doneChan

		ec.lock.Lock()

		// mark worker as available again
		ec.availWorkers[finishedTask.workerID] = true
		fmt.Printf("[Executor %d: workerCoordinator]: Worker %d just finished Task %s\n",
			ec.id, finishedTask.workerID, finishedTask.taskID)
		ec.lock.Unlock()

		go func(id string) {
			var succ bool
			if err := ec.nmClient.TaskComplete(id, &succ); err != nil {
				fmt.Errorf("[Executor: WorkerCoordinator]: Failed to invoke TaskComplete on TaskID: %s", id)
			}

			if !succ {
				fmt.Errorf("[Executor: WorkerCoordinator]: Failed to invoke TaskComplete on TaskID: %s", id)
			}
		}(finishedTask.taskID)
	}
}

func (ec *ECState) LaunchTask(task types.Task, ret *bool) error {
	ec.lock.Lock()
	defer ec.lock.Unlock()

	fmt.Printf("[Executor %d: LaunchTask]: Incoming request for Task %s:%s \n",
		ec.id, task.JobId, task.Id)

	*ret = true

	for wID := 0; wID < ec.slotCount; wID++ {
		if ec.availWorkers[wID] {
			fmt.Printf("[Executor %d: LaunchTask]: Found available worker: %d\n", ec.id, wID)
			ec.taskChans[wID] <- &task

			// this worker is no longer available
			ec.availWorkers[wID] = false
			return nil
		}
	}

	*ret = false

	fmt.Printf("[Executor %d: LaunchTask]: Request failed for Task %s:%s \n",
		ec.id, task.JobId, task.Id)
	return fmt.Errorf("All workers running")
}
