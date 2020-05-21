package main

import (
	"flag"
	"fmt"
	"log"
	"phoenix/config"
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

		mConfig := rc.MonitorConfig(i, monitor.NewNodeMonitor("", []string{}))

		log.Printf("monitor serving on %s", mConfig.Addr)
		noError(monitor.ServeMonitor(mConfig))
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
