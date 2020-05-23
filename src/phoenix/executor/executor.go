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
	doneChan chan int

	// lock
	lock sync.Mutex

	// nmClient
	nmClient *phoenix.MonitorInterface
}

// NewExecutor launches the executor backend
func NewExecutor(myID int, mySlotCount int, myNmClient *phoenix.MonitorInterface) phoenix.ExecutorServer {
	ec := &ECState{
		id:           myID,
		slotCount:    mySlotCount,
		availWorkers: make([]bool, mySlotCount),
		taskChans:    make([]chan *types.Task, mySlotCount),
		doneChan:     make(chan int, 4),
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
		go Worker(wID, ec.taskChans[wID], ec.doneChan)

		ec.availWorkers[wID] = true
	}

	// launch long-living worker coordinator
	go ec.WorkerCoordinator()
}

// WorkerCoordinator updates availWorkers
func (ec *ECState) WorkerCoordinator() {
	for {
		// find worker that has finished; block on doneChannel
		wID := <-ec.doneChan

		ec.lock.Lock()
		ec.availWorkers[wID] = true
		ec.lock.Unlock()
	}
}

func (ec *ECState) LaunchTask(task types.Task, ret *bool) error {
	ec.lock.Lock()
	defer ec.lock.Lock()

	*ret = true

	for wID := 0; wID < ec.slotCount; wID++ {
		if ec.availWorkers[wID] {
			ec.taskChans[wID] <- &task
			return nil
		}
	}

	*ret = false
	return fmt.Errorf("All workers running")
}
