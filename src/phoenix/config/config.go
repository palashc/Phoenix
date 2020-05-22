package config

import "phoenix"

type MonitorConfig struct {
	Addr    string
	Monitor phoenix.MonitorInterface
	Ready   chan<- bool
}

type TaskSchedulerConfig struct {
	Addr	string
	TaskScheduler phoenix.TaskSchedulerInterface
	Ready	chan<- bool
}

type PhoenixConfig struct {
	Schedulers []string
	Monitors   []string
}

func (pc *PhoenixConfig) MonitorConfig(i int, nm phoenix.MonitorInterface) *MonitorConfig {
	ret := new(MonitorConfig)
	ret.Addr = pc.Monitors[i]
	ret.Monitor = nm
	ret.Ready = make(chan bool, 1)
	return ret
}
