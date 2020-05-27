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
	nSlots      = flag.Int("slots", 4, "number of executor slots")
	nMonitors   = flag.Int("nm", 1, "number of monitors")
	nSchedulers = flag.Int("ns", 1, "number of scheduler")
	nFrontends  = flag.Int("nf", 1, "number of frontends")
	frc         = flag.String("config", config.DefaultConfigPath, "bin storage config file")
)

func main() {
	flag.Parse()

	p := randaddr.RandPort()

	phoenixConfig := new(config.PhoenixConfig)
	phoenixConfig.Frontends = make([]string, *nFrontends)
	phoenixConfig.Schedulers = make([]string, *nSchedulers)
	phoenixConfig.Monitors = make([]string, *nMonitors)
	phoenixConfig.NumSlots = *nSlots

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

		for i := 0; i < *nFrontends; i++ {
			host := fmt.Sprintf("%s", ipAddrs[i%nMachine])
			phoenixConfig.Frontends[i] = fmt.Sprintf("%s:%d", host, p)
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
