package main

import (
	"flag"
	"fmt"
	"log"
	"phoenix/config"
	"phoenix/executor"
	"phoenix/monitor"
)

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

	run := func(i int) {
		if i > len(rc.Monitors) {
			noError(fmt.Errorf("index out of range: %d", i))
		}

		nodeMonitorAddr := rc.Monitors[i]

		monitorClient := monitor.GetNewClient(nodeMonitorAddr)

		// default slot count = 4
		eConfig := rc.NewExecutorConfig(i, executor.NewExecutor(i, rc.NumSlots, monitorClient))

		log.Printf("executor serving on %s", eConfig.Addr)

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
