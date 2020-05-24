package executor

import (
	"fmt"
	"net/rpc"
	"phoenix/types"

	"sync"
)

type ExecutorClient struct {
	addr string
	conn *rpc.Client
	lock sync.Mutex
}

func GetNewClient(addr string) *ExecutorClient {
	return &ExecutorClient{addr: addr}
}

func (ec *ExecutorClient) rpcConn() error {
	ec.lock.Lock()
	defer ec.lock.Unlock()

	if ec.conn != nil {
		return nil
	}

	var err error
	ec.conn, err = rpc.DialHTTP("tcp", ec.addr)
	if err != nil {
		ec.conn = nil
	}
	return err
}

func (ec *ExecutorClient) LaunchTask(task types.Task, ret *bool) error {

	err := ec.rpcConn()
	fmt.Println("exec ", err)
	if err != nil {
		return err
	}
	fmt.Println("ready set go")
	err = ec.conn.Call("ECState.LaunchTask", task, ret)
	if err != nil {
		return err
	}

	return nil
}
