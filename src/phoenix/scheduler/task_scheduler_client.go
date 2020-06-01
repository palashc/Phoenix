package scheduler

import (
	"net/rpc"
	"phoenix"
	"phoenix/types"
	"sync"
	//"phoenix/types"
)

type TaskSchedulerClient struct {
	addr string
	conn *rpc.Client
	lock sync.Mutex
}

func (tsc *TaskSchedulerClient) rpcConn() error {
	tsc.lock.Lock()
	defer tsc.lock.Unlock()

	if tsc.conn != nil {
		return nil
	}

	var err error
	tsc.conn, err = rpc.DialHTTP("tcp", tsc.addr)
	if err != nil {
		tsc.conn = nil
	}
	return err
}

var _ phoenix.TaskSchedulerInterface = new(TaskSchedulerClient)

func (tsc *TaskSchedulerClient) SubmitJob(job types.Job, submitResult *bool) error {

	err := tsc.rpcConn()
	if err != nil {
		return err
	}

	err = tsc.conn.Call("TaskScheduler.SubmitJob", job, submitResult)
	if err != nil {
		return err
	}

	return nil
}

func (tsc *TaskSchedulerClient) GetTask(taskId string, taskRequest *types.TaskRequest) error {
	err := tsc.rpcConn()
	if err != nil {
		return err
	}

	err = tsc.conn.Call("TaskScheduler.GetTask", taskId, taskRequest)
	if err != nil {
		return err
	}

	return nil
}
func (tsc *TaskSchedulerClient) TaskComplete(msg types.WorkerTaskCompleteMsg, completeResult *bool) error {
	err := tsc.rpcConn()
	if err != nil {
		return err
	}

	err = tsc.conn.Call("TaskScheduler.TaskComplete", msg, completeResult)
	if err != nil {
		return err
	}

	return nil
}
