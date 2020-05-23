package main

import (
	"flag"
	"fmt"
	"log"
	"phoenix"
	"phoenix/config"
	"phoenix/executor"
	"phoenix/monitor"
	"phoenix/scheduler"
)

const DefaultSlotCount = 4

var frc = flag.String("conf", config.DefaultConfigPath, "config file")

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	flag.Parse()

	rc, e := config.LoadConfig(*frc)
	noError(e)

	schedulerClientMap := make(map[string]phoenix.TaskSchedulerInterface)
	for _, schedulerAddr := range rc.Schedulers {
		schedulerClientMap[schedulerAddr] = scheduler.GetNewTaskSchedulerClient(schedulerAddr)
	}

	run := func(i int) {
		if i > len(rc.Monitors) {
			noError(fmt.Errorf("index out of range: %d", i))
		}

		executorAddr := rc.Executors[i]
		nodeMonitorAddr := rc.Monitors[i]

		executorClient := executor.GetNewClient(executorAddr)
		monitorClient := monitor.GetNewClient(nodeMonitorAddr)

		// TODO: get a better newXXXConfig method
		mConfig := rc.NewMonitorConfig(i, monitor.NewNodeMonitor(executorClient, schedulerClientMap))

		// default slot count = 4
		eConfig := rc.NewExecutorConfig(i, executor.NewExecutor(i, DefaultSlotCount, monitorClient))

		log.Printf("monitor serving on %s", mConfig.Addr)
		log.Printf("executor serving on %s", eConfig.Addr)

		noError(monitor.ServeMonitor(mConfig))
		noError(executor.ServeExecutor(eConfig))
	}

	n := 0

	for i, _ := range rc.Monitors {
		go run(i)
		n++
	}

	if n > 0 {
		select {}
	}
}
