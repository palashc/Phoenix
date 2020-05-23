package executor

import (
	"net"
	"net/http"
	"net/rpc"
	"phoenix/config"
)

// Serve as a backend based on the given configuration
func ServeExecutor(b *config.ExecutorConfig) error {

	server := rpc.NewServer()

	// TODO, change before submission
	e := server.Register(b.Executor)
	if e != nil {
		if b.Ready != nil {
			b.Ready <- false
		}
		return e
	}

	l, e := net.Listen("tcp", b.Addr)
	if e != nil {
		if b.Ready != nil {
			b.Ready <- false
		}
		return e
	}

	if b.Ready != nil {
		b.Ready <- true
	}

	return http.Serve(l, server)
}

