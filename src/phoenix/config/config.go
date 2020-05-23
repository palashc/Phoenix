package config

import (
	"encoding/json"
	"fmt"
	"os"
	"phoenix"
)

var DefaultConfigPath = "phoenix_config.conf"

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

func (pc *PhoenixConfig) MonitorConfig(i int, nm phoenix.MonitorInterface) *MonitorConfig {
	ret := new(MonitorConfig)
	ret.Addr = pc.Monitors[i]
	ret.Monitor = nm
	ret.Ready = make(chan bool, 1)
	return ret
}

func (self *PhoenixConfig) Save(p string) error {
	b := self.marshal()

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

func (self *PhoenixConfig) String() string {
	b := self.marshal()
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

func (self *PhoenixConfig) marshal() []byte {
	b, e := json.MarshalIndent(self, "", "    ")
	if e != nil {
		panic(e)
	}

	return b
}
