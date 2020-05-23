package scheduler

import (
	"net"
	"net/http"
	"net/rpc"
	"phoenix/config"
)

func GetNewTaskSchedulerClient(addr string) (*TaskSchedulerClient, error) {
	return &(TaskSchedulerClient{
		addr: addr,
	}), nil
}

func ServeTaskScheduler(b *config.TaskSchedulerConfig) error {
	server := rpc.NewServer()
	server.Register(b.TaskScheduler)

	listener, err := net.Listen("tcp", b.Addr)
	if err != nil {
		if b.Ready != nil { //nil channel blocks
			b.Ready <- false
		}
		return err
	}

	if b.Ready != nil { //nil channel blocks
		b.Ready <- true
	}

	return http.Serve(listener, server)
}
