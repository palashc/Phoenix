package frontend

import (
	"net/rpc"
	"sync"
)

type Client struct {
	addr string
	conn *rpc.Client
	lock sync.Mutex
}

func GetNewClient(addr string) *Client {
	return &Client{addr: addr}
}

func (c *Client) rpcConn() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.conn != nil {
		return nil
	}

	var err error
	c.conn, err = rpc.DialHTTP("tcp", c.addr)
	if err != nil {
		c.conn = nil
	}
	return err
}

func (c *Client) JobComplete(jobId string, ret *bool) error {

	err := c.rpcConn()
	if err != nil {
		return err
	}

	err = c.conn.Call("Frontend.JobComplete", jobId, ret)
	if err != nil {
		return err
	}

	return nil
}
