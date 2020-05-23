package main

import (
	"flag"
	"fmt"
	"log"
	"phoenix/config"
	"phoenix/randaddr"
	"strings"
)

var (
	ips         = flag.String("ips", "localhost", "comma-seperated list of IP addresses of the set of machines that'll host monitors and schedulers")
	nMonitors   = flag.Int("nm", 1, "number of monitors")
	nSchedulers = flag.Int("ns", 1, "number of scheduler")
	deflt       = flag.Bool("default", false, "default setup of 4 monitors and 2 schedulers")
	frc         = flag.String("config", config.DefaultConfigPath, "bin storage config file")
)

func main() {
	flag.Parse()

	if *deflt {
		*nMonitors = 4
		*nSchedulers = 2
	}

	p := randaddr.RandPort()

	phoenixConfig := new(config.PhoenixConfig)
	phoenixConfig.Schedulers = make([]string, *nSchedulers)
	phoenixConfig.Monitors = make([]string, *nMonitors)

	// same number of executors as monitors
	phoenixConfig.Executors = make([]string, *nMonitors)

	ipAddrs := strings.Split(*ips, ",")
	if nMachine := len(ipAddrs); nMachine > 0 {
		for i := 0; i < *nSchedulers; i++ {
			host := fmt.Sprintf("%s", ipAddrs[i%nMachine])
			phoenixConfig.Schedulers[i] = fmt.Sprintf("%s:%d", host, p)
			p++
		}

		for i := 0; i < *nMonitors; i++ {
			host := fmt.Sprintf("%s", ipAddrs[i%nMachine])
			phoenixConfig.Monitors[i] = fmt.Sprintf("%s:%d", host, p)

			// run Executors on same machine as monitors
			phoenixConfig.Executors[i] = fmt.Sprintf("%s:%d", host, p+1000)
			p++
		}
	}

	fmt.Println(phoenixConfig.String())

	if *frc != "" {
		e := phoenixConfig.Save(*frc)
		if e != nil {
			log.Fatal(e)
		}
	}
}
