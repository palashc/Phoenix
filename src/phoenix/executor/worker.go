package executor

import (
	"fmt"
	"phoenix/types"
	"time"
)

// not exported
type taskDone struct {
	taskID   string
	workerID int
}

// Worker thread
// Main thread sends Workers Tasks through taskChan.
// Workers send "nil" back through taskChan to mark task as complete
func Worker(executorId int, workerID int, taskChan chan *types.Task, doneChan chan taskDone) {

	fmt.Printf("[Executor %d: Worker: %d] Starting Up\n", executorId, workerID)
	for {
		newTask := <-taskChan

		// synchronous method
		performTask(newTask)

		//fmt.Printf("[Executor %d: Worker: %d]: Finished Task %s:%s; waited %f ms\n",
		//	executorId, workerID, newTask.JobId, newTask.Id, newTask.T)

		// mark task as complete
		doneChan <- taskDone{newTask.Id, workerID}
	}
}

func performTask(task *types.Task) {
	//fmt.Println("task ", task.Id, " sleeping for ", task.T)
	time.Sleep(time.Millisecond * time.Duration(task.T*1000))
}
