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
	nmonitors   = flag.Int("nm", 1, "number of monitors")
	nschedulers = flag.Int("ns", 1, "number of scheduler")
	deflt       = flag.Bool("default", false, "default setup of 4 monitors and 2 schedulers")
	frc         = flag.String("config", "phoenix_config", "bin storage config file")
)

func main() {
	flag.Parse()

	if *deflt {
		*nmonitors = 4
		*nschedulers = 2
	}

	p := randaddr.RandPort()

	phoenixConfig := new(config.PhoenixConfig)
	phoenixConfig.Schedulers = make([]string, *nschedulers)
	phoenixConfig.Monitors = make([]string, *nmonitors)

	ip_addrs := strings.Split(*ips, ",")
	if nmachine := len(ip_addrs); nmachine > 0 {
		for i := 0; i < *nschedulers; i++ {
			host := fmt.Sprintf("%s", ip_addrs[i%nmachine])
			phoenixConfig.Schedulers[i] = fmt.Sprintf("%s:%d", host, p)
			p++
		}

		for i := 0; i < *nmonitors; i++ {
			host := fmt.Sprintf("%s", ip_addrs[i%nmachine])
			phoenixConfig.Monitors[i] = fmt.Sprintf("%s:%d", host, p)
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
