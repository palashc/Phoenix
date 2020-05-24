package main

import (
	"flag"
	"fmt"
	"log"
	"phoenix"
	"phoenix/config"
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

	monitorClientMap := make(map[int]phoenix.MonitorInterface)
	for index, monitorAddr := range rc.Monitors {
		monitorClientMap[index] = monitor.GetNewClient(monitorAddr)
	}

	run := func(i int) {
		if i > len(rc.Monitors) {
			noError(fmt.Errorf("index out of range: %d", i))
		}

		schedulerAddr := rc.Schedulers[i]
		sConfig := rc.NewTaskSchedulerConfig(i, scheduler.NewTaskScheduler(schedulerAddr, monitorClientMap))

		// default slot count = 4
		log.Printf("scheduler serving on %s", sConfig.Addr)
		noError(scheduler.ServeTaskScheduler(sConfig))
	}

	n := 0

	for i, _ := range rc.Schedulers {
		go run(i)
		n++
	}

	if n > 0 {
		select {}
	}
}
