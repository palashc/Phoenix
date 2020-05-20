package config

import "phoenix"

type ExecutorConfig struct {
	Addr	string
	Executor phoenix.ExecutorServer
	Ready chan<- bool
}

type MonitorConfig struct {
	Addr    string
	Monitor phoenix.MonitorInterface
	Ready   chan<- bool
}

type PhoenixConfig struct {
	Schedulers []string
	Monitors   []string
}

func (pc *PhoenixConfig) NewMonitorConfig(i int, nm phoenix.MonitorInterface) *MonitorConfig {
	ret := new(MonitorConfig)
	ret.Addr = pc.Monitors[i]
	ret.Monitor = nm
	ret.Ready = make(chan bool, 1)
	return ret
}

func (pc *PhoenixConfig) NewExecutorConfig(addr string, ec phoenix.ExecutorServer) *ExecutorConfig{
	return &ExecutorConfig{
		Addr: addr,
		Executor: ec,
	}
}
