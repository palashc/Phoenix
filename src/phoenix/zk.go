package phoenix

import "time"

const (
	ZK_WORKER_NODE_PATH           = "/workers"
	ZK_MONITOR_CONNECTION_TIMEOUT = 2 * time.Second
)

var ZkLocalServers = []string{"localhost:2181"}
// var ZkLocalServers = []string{"172.31.28.99:2181", "172.31.31.12:2181", "172.31.22.104:2181"}
