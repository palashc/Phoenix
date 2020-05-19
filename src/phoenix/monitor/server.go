package monitor

import (
	"net"
	"net/http"
	"net/rpc"
	"phoenix/config"
)

func GetNewClient(addr string) *NodeMonitorClient {
	return &NodeMonitorClient{addr: addr}
}

// Serve as a backend based on the given configuration
func ServeMonitor(b *config.MonitorConfig) error {

	server := rpc.NewServer()
	server.Register(b.Monitor)

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
