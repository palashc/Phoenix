package executor

import (

	"phoenix/types"
	"time"
)

// Worker thread
// Main thread sends Workers Tasks through taskchan.
// Workers send "nil" back through taskchan to mark task as complete
func Worker(workerID int, taskchan chan *types.Task, donechan chan int ) {

	for {
		newTask := <- taskchan
		performTask(newTask)

		// mark task as complete
		donechan <- workerID
	}
}

func performTask(task *types.Task) {
	time.Sleep(time.Millisecond * time.Duration(task.T * 1000))
}
