package worker_god

import (
	"net/rpc"
	"phoenix"
	"sync"
)

func GetNewClient(addr string) phoenix.WorkerGod {
	return &WorkerGodClient{addr: addr}
}

type WorkerGodClient struct {
	addr string
	conn *rpc.Client
	lock sync.Mutex
}

var _ phoenix.WorkerGod = &WorkerGodClient{}

func (ww *WorkerGodClient) rpcConn() error {
	ww.lock.Lock()
	defer ww.lock.Unlock()

	if ww.conn != nil {
		return nil
	}

	var err error
	ww.conn, err = rpc.DialHTTP("tcp", ww.addr)
	if err != nil {
		ww.conn = nil
	}
	return err
}

func (ww *WorkerGodClient) Kill(workerId int, submitResult *bool) error {

	err := ww.rpcConn()
	if err != nil {
		return err
	}

	return ww.conn.Call("WorkerWrapper.Kill", workerId, submitResult)
}

func (ww *WorkerGodClient) Start(workerId int, submitResult *bool) error {

	err := ww.rpcConn()
	if err != nil {
		return err
	}

	return ww.conn.Call("WorkerWrapper.Start", workerId, submitResult)
}
