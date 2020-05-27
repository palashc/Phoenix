package frontend

import (
	"net"
	"net/http"
	"net/rpc"
	"phoenix/config"
)

// Serve as a backend based on the given configuration
func ServeFrontend(b *config.FrontendConfig) error {

	server := rpc.NewServer()

	e := server.Register(b.Frontend)
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
