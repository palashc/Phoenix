package executor

import (
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
func Worker(workerID int, taskChan chan *types.Task, doneChan chan taskDone) {

	for {
		newTask := <-taskChan

		// synchronous method
		performTask(newTask)

		// mark task as complete
		doneChan <- taskDone{newTask.Id, workerID}
	}
}

func performTask(task *types.Task) {
	//fmt.Println("task ", task.Id, " sleeping for ", task.T)
	time.Sleep(time.Millisecond * time.Duration(task.T*1000))
}
