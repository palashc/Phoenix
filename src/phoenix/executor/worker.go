package executor

import (
	"phoenix/types"
	"time"
)

// Worker thread
// Main thread sends Workers Tasks through taskChan.
// Workers send "nil" back through taskChan to mark task as complete
func Worker(workerID int, taskChan chan *types.Task, doneChan chan int) {

	for {
		newTask := <-taskChan
		performTask(newTask)

		// mark task as complete
		doneChan <- workerID
	}
}

func performTask(task *types.Task) {
	time.Sleep(time.Millisecond * time.Duration(task.T*1000))
}
