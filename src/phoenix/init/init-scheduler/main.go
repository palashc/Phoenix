package main

import (
	"flag"
	"fmt"
	"log"
	"phoenix"
	"phoenix/config"
	"phoenix/frontend"

	"phoenix/monitor"
	"phoenix/scheduler"
)

const DefaultSlotCount = 4

var frc = flag.String("conf", config.DefaultConfigPath, "config file")
var schedId = flag.Int("schedId", -1, "which schedulerId to run")

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	fmt.Println("HELLOOO")
	flag.Parse()

	rc, e := config.LoadConfig(*frc)
	noError(e)

	monitorClientMap := make([]phoenix.MonitorInterface, len(rc.Monitors))
	for index, monitorAddr := range rc.Monitors {
		monitorClientMap[index] = monitor.GetNewClient(monitorAddr)
	}

	frontendClientMap := make(map[string]phoenix.FrontendInterface)
	for _, frontendAddr := range rc.Frontends {
		frontendClientMap[frontendAddr] = frontend.GetNewClient(frontendAddr)
	}

        fmt.Println("OKOK")
	run := func(i int) {
		if i > len(rc.Monitors) {
			noError(fmt.Errorf("index out of range: %d", i))
		}

		schedulerAddr := rc.Schedulers[i]
		fmt.Println("Launching scheduler at ", schedulerAddr)

		sConfig := rc.NewTaskSchedulerConfig(i,
			scheduler.NewTaskScheduler(schedulerAddr, phoenix.ZkLocalServers, frontendClientMap))

		// default slot count = 4
		log.Printf("scheduler serving on %s", sConfig.Addr)
		noError(scheduler.ServeTaskScheduler(sConfig))
	}

        fmt.Println("After run def")

	n := 0

	for i, _ := range rc.Schedulers {
		if *schedId  == -1 {
			go run(i)
			n++
		} else if *schedId == i {
			go run(i)
			n++
		}
	}

	if n > 0 {
		select {}
	}

}
