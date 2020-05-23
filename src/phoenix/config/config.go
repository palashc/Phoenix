package config

import (
	"encoding/json"
	"fmt"
	"os"
	"phoenix"
)

var DefaultConfigPath = "phoenix_config.conf"

type ExecutorConfig struct {
	Addr     string
	Executor phoenix.ExecutorServer
	Ready    chan<- bool
}

type MonitorConfig struct {
	Addr    string
	Monitor phoenix.MonitorInterface
	Ready   chan<- bool
}

type TaskSchedulerConfig struct {
	Addr          string
	TaskScheduler phoenix.TaskSchedulerInterface
	Ready         chan<- bool
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

func (pc *PhoenixConfig) NewExecutorConfig(addr string, ec phoenix.ExecutorServer) *ExecutorConfig {
	return &ExecutorConfig{
		Addr:     addr,
		Executor: ec,
	}
}

func (pc *PhoenixConfig) Save(p string) error {
	b := pc.marshal()

	fout, e := os.Create(p)
	if e != nil {
		return e
	}

	_, e = fout.Write(b)
	if e != nil {
		return e
	}

	_, e = fmt.Fprintln(fout)
	if e != nil {
		return e
	}

	return fout.Close()
}

func (pc *PhoenixConfig) String() string {
	b := pc.marshal()
	return string(b)
}

func LoadConfig(p string) (*PhoenixConfig, error) {
	fin, e := os.Open(p)
	if e != nil {
		return nil, e
	}
	defer fin.Close()

	ret := new(PhoenixConfig)
	e = json.NewDecoder(fin).Decode(ret)
	if e != nil {
		return nil, e
	}

	return ret, nil
}

func (pc *PhoenixConfig) marshal() []byte {
	b, e := json.MarshalIndent(pc, "", "    ")
	if e != nil {
		panic(e)
	}

	return b
}
