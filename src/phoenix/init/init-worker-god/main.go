package main

import (
	"flag"
	"log"
	"phoenix/config"
	workerGod "phoenix/worker-god"
)

var frc = flag.String("conf", config.DefaultConfigPath, "config file")
var isZK = flag.Bool("zk", true, "is ZK enabled?")

// if workerId is 0, run the first god
var workerId = flag.Int("workerId", 0, "which workerId to run")

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	flag.Parse()

	rc, e := config.LoadConfig(*frc)
	noError(e)

	godRc := rc.NewWorkerGodConfig(*workerId, workerGod.NewWorkerGod(rc, *isZK))

	noError(workerGod.ServeWorkerGod(godRc))
}
