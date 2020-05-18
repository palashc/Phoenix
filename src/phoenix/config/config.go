package config

import "phoenix/monitor"

type MonitorConfig struct {
	Addr    string
	Monitor monitor.NodeMonitor
	Ready   chan<- bool
}

type PhoenixConfig struct {
	Schedulers []string
	Monitors   []string
}

func (pc *PhoenixConfig) MonitorConfig(i int, nm monitor.NodeMonitor) *MonitorConfig {
	ret := new(MonitorConfig)
	ret.Addr = pc.Monitors[i]
	ret.Monitor = nm
	ret.Ready = make(chan bool, 1)
	return ret
}
