package phoenix

import "time"

const (
	ZK_WORKER_NODE_PATH           = "/workers"
	ZK_MONITOR_CONNECTION_TIMEOUT = 2 * time.Second
)

var ZkLocalServers = []string{"localhost:2181"}
