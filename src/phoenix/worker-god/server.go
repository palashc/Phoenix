package worker_god

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"phoenix/config"
)

func ServeWorkerGod(b *config.WorkerGodConfig) error {
	server := rpc.NewServer()
	server.Register(b.WorkerGod)

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

	fmt.Println("Launching WorkerGod on ", b.Addr)
	return http.Serve(listener, server)
}
