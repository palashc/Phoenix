package executor

import (
	"net/rpc"
	"phoenix/types"

	"sync"
)

type ExecutorClient struct {
	addr string
	conn *rpc.Client
	lock sync.Mutex
}

func (ec *ExecutorClient) LaunchTask(task types.Task, ret *bool) error {

	var err error
	if ec.conn != nil {
		ec.lock.Lock()
		ec.conn, err = rpc.DialHTTP("tcp", ec.addr)
		ec.lock.Unlock()

		if err != nil {
			return err
		}
	}

	if err = ec.conn.Call("Executor.launchTask", task, ret); err != nil {

		ec.lock.Lock()
		ec.conn, err = rpc.DialHTTP("tcp", ec.addr)
		ec.lock.Unlock()

		if ec.conn, err = rpc.DialHTTP("tcp", ec.addr); err != nil {
			return err
		}

		return ec.conn.Call("Executor.launchTask", task, ret)
	}

	return nil
}
