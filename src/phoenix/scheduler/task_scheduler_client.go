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

func (tsc *TaskSchedulerClient) GetTask(taskId string, task *types.Task) error {
	err := tsc.rpcConn()
	if err != nil {
		return err
	}

	err = tsc.conn.Call("TaskScheduler.GetTask", taskId, task)
	if err != nil {
		return err
	}

	return nil
}
func (tsc *TaskSchedulerClient) NotifyTaskComplete(taskId string, completeResult *bool) error {
	err := tsc.rpcConn()
	if err != nil {
		return err
	}

	err = tsc.conn.Call("TaskScheduler.NotifyTaskComplete", taskId, completeResult)
	if err != nil {
		return err
	}

	return nil
}
