package monitor

import (
	"net/rpc"
	"phoenix/types"
	"sync"
)

type NodeMonitorClient struct {
	addr string
	conn *rpc.Client
	lock sync.Mutex
}

func (nmc *NodeMonitorClient) rpcConn() error {
	nmc.lock.Lock()
	defer nmc.lock.Unlock()

	if nmc.conn != nil {
		return nil
	}

	var err error
	nmc.conn, err = rpc.DialHTTP("tcp", nmc.addr)
	if err != nil {
		nmc.conn = nil
	}
	return err
}

func (nmc *NodeMonitorClient) EnqueueReservation(taskR *types.TaskReservation, pos *int) error {

	err := nmc.rpcConn()
	if err != nil {
		return err
	}

	err = nmc.conn.Call("NodeMonitor.HandleEnqueueReservation", taskR, pos)
	if err != nil {
		return err
	}

	return nil
}

func (nmc *NodeMonitorClient) Probe(_ignore int, n *int) error {

	err := nmc.rpcConn()
	if err != nil {
		return err
	}

	err = nmc.conn.Call("NodeMonitor.GetNumQueuedTasks", _ignore, n)
	if err != nil {
		return err
	}

	return nil
}

func (nmc *NodeMonitorClient) TaskComplete(taskID string, ret *bool) error {

	err := nmc.rpcConn()
	if err != nil {
		return err
	}

	err = nmc.conn.Call("NodeMonitor.HandleTaskComplete", taskID, ret)
	if err != nil {
		return err
	}

	return nil
}

func (nmc *NodeMonitorClient) CancelTaskReservation(taskID string, ret *bool) error {

	err := nmc.rpcConn()
	if err != nil {
		return err
	}

	err = nmc.conn.Call("NodeMonitor.HandleCancelTaskReservation", taskID, ret)
	if err != nil {
		return err
	}

	return nil
}
