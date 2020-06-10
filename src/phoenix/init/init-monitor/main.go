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

var frc = flag.String("conf", config.DefaultConfigPath, "config file")

// if -1, run all workers
var workerId = flag.Int("workerId", -1, "which workerId to run")
var isZK = flag.Bool("zk", true, "is ZK enabled?")

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

		executorClient := executor.GetNewClient(executorAddr)

		// TODO: get a better newXXXConfig method
		monitorAddr := rc.Monitors[i]
		mConfig := rc.NewMonitorConfig(i, monitor.NewNodeMonitor(rc.NumSlots, executorClient,
			schedulerClientMap, monitorAddr, *isZK, phoenix.ZkLocalServers))

		log.Printf("monitor serving on %s", mConfig.Addr)

		noError(monitor.ServeMonitor(mConfig))
	}

	// run all monitors
	if *workerId == -1 {
		for i, _ := range rc.Monitors {
			go run(i)
		}
	} else {
		run(*workerId)
	}

	select {}

}
