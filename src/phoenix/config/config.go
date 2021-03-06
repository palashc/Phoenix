package config

import (
	"encoding/json"
	"fmt"
	"os"
	"phoenix"
)

var DefaultConfigPath = "phoenix_config.conf"

type FrontendConfig struct {
	Addr     string
	Frontend phoenix.FrontendInterface
	Ready    chan bool
}

type ExecutorConfig struct {
	Addr     string
	Executor phoenix.ExecutorInterface
	Ready    chan bool
}

type MonitorConfig struct {
	Addr    string
	Monitor phoenix.MonitorInterface
	Ready   chan bool
}

type TaskSchedulerConfig struct {
	Addr          string
	TaskScheduler phoenix.TaskSchedulerInterface
	Ready         chan bool
}

type WorkerGodConfig struct {
	Addr      string
	WorkerGod phoenix.WorkerGod
	Ready     chan bool
}

//type JobGeneratorConfig struct {
//	Seed			int
//	TaskDuration	int
//
//	// Extending for future testing file delivery
//	UseTaskFile		bool
//	TaskFileName	string
//}

type PhoenixConfig struct {
	NumSlots   int
	Frontends  []string
	Schedulers []string
	Monitors   []string
	Executors  []string
	WorkerGods []string
}

func (pc *PhoenixConfig) NewFrontendConfig(i int, fe phoenix.FrontendInterface) *FrontendConfig {
	return &FrontendConfig{
		Addr:     pc.Frontends[i],
		Frontend: fe,
		Ready:    make(chan bool, 1),
	}
}

func (pc *PhoenixConfig) NewTaskSchedulerConfig(i int, ts phoenix.TaskSchedulerInterface) *TaskSchedulerConfig {
	return &TaskSchedulerConfig{
		Addr:          pc.Schedulers[i],
		TaskScheduler: ts,
		Ready:         make(chan bool, 1),
	}
}

func (pc *PhoenixConfig) NewMonitorConfig(i int, nm phoenix.MonitorInterface) *MonitorConfig {
	ret := new(MonitorConfig)
	ret.Addr = pc.Monitors[i]
	ret.Monitor = nm
	ret.Ready = make(chan bool, 1)
	return ret
}

func (pc *PhoenixConfig) NewExecutorConfig(i int, ec phoenix.ExecutorInterface) *ExecutorConfig {
	return &ExecutorConfig{
		Addr:     pc.Executors[i],
		Executor: ec,
		Ready:    make(chan bool, 1),
	}
}

func (pc *PhoenixConfig) NewWorkerGodConfig(i int, ww phoenix.WorkerGod) *WorkerGodConfig {
	return &WorkerGodConfig{
		Addr:      pc.WorkerGods[i],
		WorkerGod: ww,
		Ready:     make(chan bool, 1),
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
func (pc *PhoenixConfig) Write(p string) (*os.File, error) {
	b := pc.marshal()

	fout, e := os.Create(p)
	if e != nil {
		return nil, e
	}

	_, e = fout.Write(b)
	if e != nil {
		return nil, e
	}

	return fout, nil
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
