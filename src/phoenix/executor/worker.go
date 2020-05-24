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
func Worker(workerID int, taskChan chan types.Task, doneChan chan taskDone) {

	for {
		newTask := <-taskChan
		fmt.Println("got task ", newTask)
		// synchronous method
		performTask(newTask)

		// mark task as complete
		fmt.Println("sending on done")
		doneChan <- taskDone{newTask.Id, workerID}
	}
}

func performTask(task types.Task) {
	fmt.Println("start")
	time.Sleep(time.Millisecond * time.Duration(task.T*1000))
	fmt.Println("end")
}
